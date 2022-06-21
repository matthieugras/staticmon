module EventGenerators (getGenerator) where

import Control.Monad (forM_, replicateM_)
import Data.Text qualified as T
import EventPrinting
import System.IO (FilePath)

getGenerator :: T.Text -> (FilePath -> IO ())
getGenerator genid =
  case genid of
    "simple_pred_1" -> simplePred1Gen
    _ -> error "unknown generator"

simplePred1Gen :: FilePath -> IO ()
simplePred1Gen fp = withPrintState fp $ do
  forM_
    [0 .. 1000000]
    ( \i -> do
        newDb i
        outputNewEvent "A" [Intgr 5, Intgr 10]
    )
  endOutput
