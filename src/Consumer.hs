{-# LANGUAGE OverloadedStrings, DataKinds #-}
module Consumer where

import Network.AMQP
import qualified Data.Text as T
import qualified Data.ByteString.Lazy.Char8 as BL
import Database.Cassandra.CQL (Pool)
import Cassandra
import Models
import Github
import Control.Applicative ((<*>))
import Control.Concurrent (forkIO)

initRabbit :: Pool -> IO (Connection, Channel)
initRabbit pool = do
  conn <- openConnection "127.0.0.1" "/" "guest" "guest"
  chan <- openChannel conn

    -- declare a queue, exchange and binding
  declareQueue chan newQueue {queueName = "userToStar"}
  declareExchange chan newExchange {exchangeName = "star", exchangeType = "direct"}
  bindQueue chan "userToStar" "myExchange" "myKey"

    -- subscribe to the queue
  consumeMsgs chan "myQueue" Ack $ starCB pool 0

  return (conn, chan)

starCB :: Pool -> Int -> (Message, Envelope) -> IO ()
starCB pool ts (msg, env) = do
  forkIO $ starRepos pool ts msg
  ackEnv env

starRepos :: Pool -> Int -> Message -> IO ()
starRepos pool ts msg = do
  users <- findEvents pool ("user", ts)
  case users of [] -> return ()
                xs -> (starRepos' pool msg xs) >> starRepos pool (lastTS xs) msg

lastTS :: [Event] -> Int
lastTS = ts . last

starRepos' :: Pool -> Message -> [Event] -> IO ()
starRepos' pool msg users = do
  putStrLn $ "received message: " ++ (BL.unpack $ msgBody msg)
  uname <- return $ T.pack $ BL.unpack $ msgBody msg
  repos <- findRepos pool (uname, False)
  starRepoFns <- createStarFns uname users repos
  let fns = case starRepoFns of Nothing -> [return ""]
                                Just xs -> xs
                                _ -> [return ""]
  sequence_ fns

createStarFns :: T.Text -> [Event] -> [Repo] -> IO (Maybe [IO String])
createStarFns uname [] repos = return Nothing
createStarFns uname users repos = do
  accessTokens <- return $ map (\user -> data2 user) users
  starRepoFns <- return $ map (\repo -> starRepo uname (rname repo)) repos
  return $ Just $ starRepoFns <*> accessTokens