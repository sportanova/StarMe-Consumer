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
  -- putStrLn $ "OLD MESSAGE: " ++ head (splitOn ":" (BL.unpack $ msgBody msg))
  putStrLn $ "OLD MESSAGE: " ++ (BL.unpack $ msgBody msg)
  starOldRepos pool (BL.unpack $ msgBody msg)
  ackEnv env

type OldStarPush = String -> IO ()

starCB :: OldStarPush -> Pool -> Int -> (Message, Envelope) -> IO ()
starCB oldStarPush pool ts (msg, env) = do
  forkIO $ starRepos oldStarPush pool ts msg
  ackEnv env

createStarFns :: T.Text -> [Event] -> [Repo] -> Maybe [IO String]
createStarFns _ [] _ = Nothing
createStarFns uname users repos =
  let accessTokens = map (\user -> data2 user) users
      starRepoFns = map (\repo -> starRepo uname (rname repo)) repos
  in Just $ starRepoFns <*> accessTokens

starRepos'' :: User -> [Event] -> [Repo] -> IO ()
starRepos'' user users repos =
  case createStarFns (username user) users repos of Just fns -> sequence_ fns
                                                    Nothing -> return ()

lastTS :: [Event] -> Int
lastTS = ts . last

starRepos' :: OldStarPush -> Pool -> Int -> T.Text -> Maybe User -> [Repo] -> IO ()
starRepos' oldStarPush pool ts uname (Just user) repos = do
  users <- findEvents pool ("user", ts)
  putStrLn $ "here " ++ (show user)
  case users of [] -> return ()
                xs -> sendOldReposToQueue oldStarPush user users >> (starRepos'' user xs repos) >> starRepos' oldStarPush pool (lastTS xs) uname (Just user) repos
starRepos' _ _ _ _ Nothing _= return ()

starRepos :: OldStarPush -> Pool -> Int -> Message -> IO ()
starRepos oldStarPush pool ts msg = do
  uname <- return $ T.pack $ BL.unpack $ msgBody msg
  user <- findUser pool uname
  repos <- case user of Just u -> findRepos pool (username u, False)
                        Nothing -> return []
  starRepos' oldStarPush pool ts uname user repos

sendOldReposToQueue :: OldStarPush -> User -> [Event] -> IO () -- send to queue for new user to star all old users' repos
sendOldReposToQueue oldStarPush user users = sequence_ $ map (\u -> oldStarPush $ T.unpack (data1 u) ++ ":" ++ T.unpack (token user)) users -- userId:accessToken

starOldRepos :: Pool -> String -> IO ()
starOldRepos pool userIdAndToken = do
  username:token <- return $ splitOn ":" userIdAndToken
  accessToken <- return (T.pack $ concat token)
  repos <- findRepos1 pool (T.pack username)
  mapM_ (\repo -> starRepo (rusername repo) (rname repo) accessToken) repos
    
  
  
  
  
  
  
  

  