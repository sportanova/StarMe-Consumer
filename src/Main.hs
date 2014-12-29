module Main where

import Consumer
import Network.AMQP
import Control.Concurrent(threadDelay)
import Control.Monad(forever)
import Cassandra

main::IO (Connection, Channel)
main = do
  pool <- initCass
  rabbit <- initRabbit pool
  forever delay

delay = threadDelay 1000000000