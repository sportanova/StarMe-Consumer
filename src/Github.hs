{-# LANGUAGE OverloadedStrings, DataKinds #-}
module Github where

import qualified Data.Text as T
import Network.HTTP
import Network.URI
import Network.HTTP.Conduit
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Char8 as C
import Cassandra
import Data.Maybe
import Control.Monad.IO.Class(liftIO)
import Database.Cassandra.CQL
import Models
import Data.Aeson.Types
import Data.Aeson

-- https://github.com/login/oauth/authorize?scope=user,public_repo&client_id=99c89395ab6f347787e8

-- PUT /user/starred/:owner/:repo
starRepo owner repo accessToken = do
  initReq <- parseUrl $ "https://api.github.com/user/starred/" ++ T.unpack owner ++ "/" ++ T.unpack repo ++ "?access_token=" ++ T.unpack accessToken
  let req = initReq { checkStatus = checkStatusFn, secure = True, method = "PUT", requestHeaders = [("Accept", "application/vnd.github.v3+json"), ("User-Agent", "StarMe")] } -- Turn on https
  response <- withManager $ httpLbs req
  -- headers <- return (responseHeaders response) -- TODO: evaluate header for 204 success code
  L.putStrLn $ responseBody response
  putStrLn $ "repo: " ++ (T.unpack repo)
  return ""

checkStatusFn = 
  \_ _ _ -> Nothing

createRepoListURL :: String -> String
createRepoListURL username = "https://api.github.com/users/" ++ username ++ "/repos?sort=updated"

getGHRepos :: String -> IO (Maybe [Repo])
getGHRepos username = do
  initReq <- parseUrl $ createRepoListURL username
  let req = initReq { secure = True, method = "GET", requestHeaders = [("Accept", "application/vnd.github.v3+json"), ("User-Agent", "StarMe")] } -- Turn on https
  response <- withManager $ httpLbs req
  headers <- return (responseHeaders response)
  return $ decode (responseBody response)

createAuthPostURL :: T.Text -> String
createAuthPostURL code = "https://github.com/login/oauth/access_token?client_id=99c89395ab6f347787e8&client_secret=74c2b3119b1a0aa39a8482dc116ada1c870ea80f&code=" ++ T.unpack code ++ "&redirect_uri=http://localhost:3000/auth_cb"

createNewUser :: Pool -> T.Text -> IO (Maybe User)
createNewUser pool code = getAccessToken code >>= (\at -> (getUserInfo at) >>= (\u -> insertUser pool u))

getAccessToken :: T.Text -> IO (Maybe AccessToken)
getAccessToken param = do
  initReq <- parseUrl $ createAuthPostURL param
  let req' = initReq { secure = True, method = "POST", requestHeaders = [("Accept", "application/json")] } -- Turn on https
  let req = urlEncodedBody [("?nonce:", "2"), ("&method", "getInfo")] req'
  response <- withManager $ httpLbs req
  L.putStr $ responseBody response
  return $ decode (responseBody response)

getUserInfo :: Maybe AccessToken -> IO (Maybe User)
getUserInfo (Just at) = do
  initReq <- parseUrl "https://api.github.com/user"
  let req = initReq { secure = True, method = "GET", requestHeaders = [("Authorization", C.pack("Bearer " ++ T.unpack (accessToken at))), ("User-Agent", "StarMe")] } -- Turn on https
  response <- withManager $ httpLbs req
  return $ addTokenToUser (decode (responseBody response)) (accessToken at)
getUserInfo Nothing = return Nothing

addTokenToUser :: Maybe User -> T.Text -> Maybe User
addTokenToUser (Just user) token = Just user {token = token}
addTokenToUser Nothing token = Nothing