{-# LANGUAGE OverloadedStrings, DataKinds #-}
module Cassandra where

import Database.Cassandra.CQL
import Data.UUID
import System.Random
import qualified Data.Text as T
import Data.ByteString.Char8 (ByteString)
import Control.Monad.IO.Class(liftIO)
import qualified Models as M
import Data.Int(Int64)
import Data.Time.Clock.POSIX

initCass :: IO Pool
initCass = createPool >>= (\pool -> runCas pool (executeSchema ONE createUsersTable () >> executeSchema ONE createReposTable () >> executeSchema ONE createEventsTable ())
  >> return pool)

createPool :: IO Pool
createPool = newPool [("localhost", "9042")] "starme" Nothing -- servers, keyspace, maybe auth

createKeyspace :: IO (Query Schema () ())
createKeyspace = return "CREATE KEYSPACE IF NOT EXISTS starme WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };"

createUsersTable :: Query Schema () ()
createUsersTable = "CREATE TABLE IF NOT EXISTS users (username text PRIMARY KEY, id int, url text, name text, a_token text, password text)"

insertUser' :: (T.Text, Int, T.Text, T.Text, T.Text, T.Text) -> Cas ()
insertUser' (username, id, url, name, a_token, password) = executeWrite ONE q (username, id, url, name, a_token, password)
  where q = "INSERT INTO users (username, id, url, name, a_token, password) values (?, ?, ?, ?, ?, ?)"

insertUser :: Pool -> Maybe M.User -> IO (Maybe M.User)
insertUser pool (Just user) = (runCas pool $ insertUser' values) >> return (Just user)
  where values = (M.username user, M.id user, M.url user, M.name user, M.token user, "")
insertUser pool Nothing = return (Nothing)

findUser' :: (MonadCassandra m) => T.Text -> m (Maybe (T.Text, Int, T.Text, T.Text, T.Text, T.Text))
findUser' username = executeRow ONE q username
  where q = "SELECT username, id, url, name, a_token, password FROM users WHERE username=?"

findUser :: Pool -> T.Text -> IO (Maybe M.User)
findUser pool username = runCas pool $ (findUser' username) >>= (\user -> return $ convertToUser user)

convertToUser :: Maybe (T.Text, Int, T.Text, T.Text, T.Text, T.Text) -> Maybe M.User
convertToUser (Just (username, id, url, name, accessToken, password)) = Just user
  where user = M.User {M.username = username, M.id = id, M.url = url, M.name = name, M.token = accessToken, M.password = password}
convertToUser Nothing = Nothing

createEventsTable :: Query Schema () ()
createEventsTable = "CREATE TABLE IF NOT EXISTS events (name text, ts int, data1 text, data2 text, PRIMARY KEY(name, ts))"

insertEvent' :: (T.Text, Int, T.Text, T.Text) -> Cas ()
insertEvent' (event, ts, username, a_token) = executeWrite ONE q (event, ts, username, a_token)
  where q = "INSERT INTO events (name, ts, data1, data2) values (?, ?, ?, ?)"

insertEvent :: Pool -> Maybe M.User -> IO (Maybe M.User)
insertEvent pool (Just user) = (do
  time <- round `fmap` getPOSIXTime
  let values = ("user", time, M.username user, M.token user)
  runCas pool $ insertEvent' values) >> return (Just user)
insertEvent pool Nothing = return Nothing

convertToEvent :: (T.Text, Int, T.Text, T.Text) -> M.Event
convertToEvent (event, time, data1, data2) = M.Event {M.ename = event, M.ts = time, M.data1 = data1, M.data2 = data2}

findEvents' :: (MonadCassandra m) => (T.Text, Int) -> m [(T.Text, Int, T.Text, T.Text)]
findEvents' tuple = executeRows ONE q tuple
  where q = "SELECT name, ts, data1, data2 FROM events WHERE name=? AND ts>? limit 50"

findEvents :: Pool -> (T.Text, Int) -> IO [M.Event]
findEvents pool tuple = runCas pool $ fmap (\tup -> map convertToEvent tup) (findEvents' tuple)

createReposTable :: Query Schema () ()
createReposTable = "CREATE TABLE IF NOT EXISTS repos (username text, name text, starred boolean, PRIMARY KEY(username, starred, name))"

insertRepo' :: (T.Text, T.Text, Bool) -> Cas ()
insertRepo' (username, name, starred) = executeWrite ONE q (username, name, starred)
  where q = "INSERT INTO repos (username, name, starred) values (?, ?, ?)"

insertRepo :: Pool -> T.Text -> T.Text -> Bool -> IO ()
insertRepo pool username name starred = (runCas pool $ insertRepo' values)
  where values = (username, name, starred)

insertRepos :: Pool -> [M.Repo] -> [IO ()]
insertRepos pool repos = map (\repo -> insertRepo pool (M.rusername repo) (M.rname repo) False) repos

convertToRepo :: (T.Text, Bool, T.Text) -> M.Repo
convertToRepo (username, starred, name) = M.Repo {M.rusername = username, M.starred = starred, M.rname = name}

findRepos' :: (MonadCassandra m) => (T.Text, Bool) -> m ([(T.Text, Bool, T.Text)])
findRepos' tuple = executeRows ONE q tuple
  where q = "SELECT username, starred, name FROM repos WHERE username=? AND starred=?"

findRepos :: Pool -> (T.Text, Bool) -> IO ([M.Repo])
findRepos pool tuple = runCas pool $ fmap (\tup -> map convertToRepo tup) (findRepos' tuple)

findRepos1' :: (MonadCassandra m) => T.Text -> m ([(T.Text, Bool, T.Text)])
findRepos1' tuple = executeRows ONE q tuple
  where q = "SELECT username, starred, name FROM repos WHERE username=? LIMIT 50"

findRepos1 :: Pool -> T.Text -> IO ([M.Repo])
findRepos1 pool tuple = runCas pool $ fmap (\tup -> map convertToRepo tup) (findRepos1' tuple)