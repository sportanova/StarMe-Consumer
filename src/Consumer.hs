{-# LANGUAGE OverloadedStrings, BangPatterns #-}
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
  conn <- openConnection "127.0.0.1" "/" "stephen" "stephen"
  chan <- openChannel conn

    -- declare a queue, exchange and binding
  declareQueue chan newQueue {queueName = "userToStar"}
  declareExchange chan newExchange {exchangeName = "starRepos", exchangeType = "topic"}
  bindQueue chan "userToStar" "star" "star.new.user"
  
    -- subscribe to the queue
  consumeMsgs chan "userToStar" Ack $ starCB pool 0
  
  declareQueue chan newQueue {queueName = "oldUsers"}
  bindQueue chan "oldUsers" "starRepos" "star.old.user"
  
  consumeMsgs chan "oldUsers" Ack $ starOldUsersCB pool 0

  return (conn, chan)

pushMessage :: Channel -> T.Text -> T.Text -> String -> IO ()
pushMessage chan exchange key msg = publishMsg chan exchange key newMsg {msgBody = (BL.pack msg), msgDeliveryMode = Just Persistent}

starOldUsersCB :: Pool -> Int -> (Message, Envelope) -> IO ()
starOldUsersCB pool ts (msg, env) = do
  putStrLn $ "OLD MESSAGE: " ++ (BL.unpack $ msgBody msg)
  ackEnv env

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

starOldRepos :: Pool -> T.Text -> [Event] -> IO ()
starOldRepos pool token users = do
  repos' <- sequence $ map (\user -> findRepos pool (data1 user, False)) users
  repos <- return $ concat repos'
  return ()

starRepos' :: Pool -> Message -> [Event] -> IO ()
starRepos' pool msg users = do
  putStrLn $ "received message: " ++ (BL.unpack $ msgBody msg)
  uname <- return $ T.pack $ BL.unpack $ msgBody msg
  token <- return $ findUser pool uname
  repos <- findRepos pool (uname, False)
  starRepoFns <- createStarFns uname users repos
  let fns = case starRepoFns of Nothing -> [return ""]
                                Just xs -> xs
                                _ -> [return ""]
  sequence_ fns

createStarFns :: T.Text -> [Event] -> [Repo] -> IO (Maybe [IO String])
createStarFns _ [] _ = return Nothing
createStarFns uname users repos = do
  accessTokens <- return $ map (\user -> data2 user) users
  starRepoFns <- return $ map (\repo -> starRepo uname (rname repo)) repos
  return $ Just $ starRepoFns <*> accessTokens