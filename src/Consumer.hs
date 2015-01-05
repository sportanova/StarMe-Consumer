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
import Data.List.Split

initRabbit :: Pool -> IO (Connection, Channel)
initRabbit pool = do
  conn <- openConnection "127.0.0.1" "/" "stephen" "stephen"
  chan <- openChannel conn

    -- declare a queue, exchange and binding
  declareQueue chan newQueue {queueName = "userToStar"}
  declareExchange chan newExchange {exchangeName = "starRepos", exchangeType = "topic"}
  bindQueue chan "userToStar" "star" "star.new.user"
  
    -- subscribe to the queue
  let oldStarPush = pushMessage chan "starRepos" "star.old.user"
  consumeMsgs chan "userToStar" Ack $ starCB oldStarPush pool 0

  declareQueue chan newQueue {queueName = "oldUsers"}
  bindQueue chan "oldUsers" "starRepos" "star.old.user"

  consumeMsgs chan "oldUsers" Ack $ starOldReposCB pool 0

  return (conn, chan)

pushMessage :: Channel -> T.Text -> T.Text -> String -> IO ()
pushMessage chan exchange key msg = publishMsg chan exchange key newMsg { msgBody = (BL.pack msg), msgDeliveryMode = Just Persistent }

starOldReposCB :: Pool -> Int -> (Message, Envelope) -> IO ()
starOldReposCB pool ts (msg, env) = do
  putStrLn $ "OLD MESSAGE: " ++ head (splitOn ":" (BL.unpack $ msgBody msg))
  ackEnv env

type OldStarPush = String -> IO ()

starCB :: OldStarPush -> Pool -> Int -> (Message, Envelope) -> IO ()
starCB oldStarPush pool ts (msg, env) = do
  forkIO $ starRepos oldStarPush pool ts msg
  ackEnv env

createStarFns :: T.Text -> [Event] -> [Repo] -> IO (Maybe [IO String])
createStarFns _ [] _ = return Nothing
createStarFns uname users repos = do
  accessTokens <- return $ map (\user -> data2 user) users
  starRepoFns <- return $ map (\repo -> starRepo uname (rname repo)) repos
  return $ Just $ starRepoFns <*> accessTokens

starRepos'' :: Pool -> User -> [Event] -> IO ()
starRepos'' pool user users = do
  putStrLn $ "received message: " ++ T.unpack (username user)
  repos <- findRepos pool (username user, False)
  starRepoFns <- createStarFns (username user) users repos
  let fns = case starRepoFns of Nothing -> [return ""]
                                Just xs -> xs
                                _ -> [return ""]
  sequence_ fns

lastTS :: [Event] -> Int
lastTS = ts . last

starRepos' :: OldStarPush -> Pool -> Int -> T.Text -> Maybe User -> IO ()
starRepos' oldStarPush pool ts uname (Just user) = do
  users <- findEvents pool ("user", ts)
  putStrLn $ "here " ++ (show user)
  case users of [] -> return ()
                xs -> {- starOldRepos oldStarPush () >> -} (starRepos'' pool user xs) >> starRepos' oldStarPush pool (lastTS xs) uname (Just user)
starRepos' oldStarPush pool ts msg Nothing = return ()

starRepos oldStarPush pool ts msg = do
  uname <- return $ T.pack $ BL.unpack $ msgBody msg
  user <- findUser pool uname
  starRepos' oldStarPush pool ts uname user

starOldRepos :: OldStarPush -> User -> [Event] -> IO ()
starOldRepos oldStarPush user users = do
  userIds <- sequence $ map (\u -> oldStarPush $ T.unpack (data1 u)) users
  --repos' <- sequence $ map (\user -> findRepos pool (data1 user, False)) users
  --repos <- return $ concat repos'
  return ()

{- starOldRepos :: Pool -> T.Text -> [Event] -> IO ()
starOldRepos pool token users = do
  repos' <- sequence $ map (\user -> findRepos pool (data1 user, False)) users
  repos <- return $ concat repos'
  return ()
  -}
    
  
  
  
  
  
  
  

  