{-# LANGUAGE OverloadedStrings, DataKinds #-}
module Models where

import Control.Monad (mzero)
import Data.Aeson.Types
import Data.Aeson
import qualified Data.Text as T
import Control.Applicative ((<$>), (<*>))
import qualified Data.ByteString.Lazy as L

data Event = Event {ename :: T.Text, ts :: Int, data1 :: T.Text, data2 :: T.Text} deriving Show

instance FromJSON Event where
  parseJSON (Object v) = Event <$>
                         v .: "name" <*>
                         v .: "ts" <*>
                         v .: "data1" <*>
                         v .: "data2"
  parseJSON _ = mzero

instance ToJSON Event where
  toJSON (Event name ts data1 data2) =
    object ["ename" .= name, "ts" .= ts, "data1" .= data1, "data2" .= data2]

data Repo = Repo {rname :: T.Text, rusername :: T.Text, starred :: Bool} deriving Show

instance FromJSON Repo where
  parseJSON (Object v) = Repo <$>
                         v .: "name" <*>
                         v .:? "username" .!= "" <*>
                         v .:? "starred" .!= False
  parseJSON _ = mzero

instance ToJSON Repo where
  toJSON (Repo name username starred) =
    object ["name" .= name, "username" .= username, "starred" .= starred]

data User = User {
                   username :: T.Text,
                   id :: Int,
                   url :: T.Text,
                   name :: T.Text,
                   token :: T.Text,
                   password :: T.Text
                 }

convertBodyToJSON :: L.ByteString -> Maybe [Repo]
convertBodyToJSON body = do
  decode body

instance ToJSON User where
  toJSON (User username id url name token pw) =
    object ["username" .= username, "id" .= id, "url" .= url, "name" .= name, "token" .= token, "password" .= pw]

instance FromJSON User where
  parseJSON (Object v) = User <$>
                         v .: "login" <*>
                         v .: "id" <*>
                         v .: "url" <*>
                         v .: "name" <*>
                         v .:? "token" .!= "" <*>
                         v .:? "password" .!= ""
  parseJSON _ = mzero

data AccessToken = AccessToken { accessToken :: T.Text,
                                 tokenType :: T.Text,
                                 scope :: T.Text
                               } deriving Show

instance ToJSON AccessToken where
  toJSON (AccessToken accessToken tokenType scope) = object ["accessToken" .= accessToken, "tokenType" .= tokenType, "scope" .= scope]
  
instance FromJSON AccessToken where
  parseJSON (Object v) = AccessToken <$>
                         v .: "access_token" <*>
                         v .: "token_type" <*>
                         v .: "scope"
  parseJSON _ = mzero