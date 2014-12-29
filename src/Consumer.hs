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

initRabbit :: Pool -> IO (Connection, Channel)
initRabbit pool = do
  conn <- openConnection "127.0.0.1" "/" "guest" "guest"
  chan <- openChannel conn

    -- declare a queue, exchange and binding
  declareQueue chan newQueue {queueName = "myQueue"}
  declareExchange chan newExchange {exchangeName = "myExchange", exchangeType = "direct"}
  bindQueue chan "myQueue" "myExchange" "myKey"

    -- subscribe to the queue
  consumeMsgs chan "myQueue" Ack $ starUserRepos pool

  return (conn, chan)

starUserRepos :: Pool -> (Message, Envelope) -> IO ()
starUserRepos pool (msg, env) = do
  putStrLn $ "received message: " ++ (BL.unpack $ msgBody msg)
  uname <- return $ T.pack $ BL.unpack $ msgBody msg
  repos <- findRepos pool (uname, False)
  users <- findEvents pool ("user", 0)
  accessTokens <- return $ map (\user -> data2 user) users
  starRepoFns <- return $ map (\repo -> starRepo uname (rname repo)) repos
  sequence $ starRepoFns <*> accessTokens
  ackEnv env