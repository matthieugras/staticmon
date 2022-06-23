{-# LANGUAGE ParallelListComp #-}

module EventGenerators (getGenerator) where

import Control.Monad (forM_, replicateM_)
import Data.Int (Int64)
import Data.Text qualified as T
import EventPrinting
import System.IO (FilePath)

intArgs is = map Intgr is

getGenerator :: T.Text -> (FilePath -> IO ())
getGenerator genid =
  case genid of
    "simple_pred_1" -> simplePred1Gen
    "rel_ops1" -> relOps1Gen
    _ -> error "unknown generator"

--  A(c,a,b) AND (D(c,b) AND (B(b,a) OR C(a,b)))
relOps1Gen :: FilePath -> IO ()
relOps1Gen fp = withPrintState fp $ do
  forM_ inps $ \(i, a, b, c) -> do
    newDb i
    outputNewEvent "A" (intArgs [c, a, b])
    outputNewEvent "D" (intArgs [c, b])
    outputNewEvent "B" (intArgs [b, a])
    outputNewEvent "C" (intArgs [a, b])
  endOutput
  where
    inps =
      [ (i, a, b, c)
        | i <- [0 .. ub]
        | a <- [1 .. ub]
        | b <- [2 .. ub]
        | c <- [3 .. ub]
      ]
    ub = 100000 :: Int64

-- A(a,b)
simplePred1Gen :: FilePath -> IO ()
simplePred1Gen fp = withPrintState fp $ do
  forM_ [0 .. 1000000] $ \i -> do
    newDb i
    outputNewEvent "A" (intArgs [5, 10])
  endOutput
