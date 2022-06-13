module EventPrinting
  ( withPrintState,
    EventData (..),
    outputNewEvent,
    OutpS,
    newDb,
    endOutput,
  )
where

import Control.Applicative (liftA2)
import Control.Exception (assert)
import Control.Monad (when)
import Control.Monad.State.Strict qualified as S
import Data.Foldable (foldr, toList)
import Data.Function ((&))
import Data.Int
import Data.List (intersperse)
import Data.Maybe (fromJust, maybe)
import Data.Text qualified as T
import Data.Text.IO qualified as TIO
import Fmt
import System.IO (FilePath, Handle, IOMode (..), withFile)

default (T.Text)

data EventData = Str !T.Text | Dbl !Double | Intgr !Int64

instance Buildable EventData where
  build (Str s) = build s
  build (Dbl d) = build d
  build (Intgr i) = build i

data OutputState = OutputState
  { outHandle :: Handle,
    outEmpty :: Bool
  }

type OutpS = S.StateT OutputState IO

endOutput :: OutpS ()
endOutput = endLine

withPrintState :: FilePath -> OutpS () -> IO ()
withPrintState outf action =
  withFile
    outf
    WriteMode
    ( \outHandle ->
        S.evalStateT
          action
          OutputState
            { outHandle,
              outEmpty = True
            }
    )

newDb :: Int64 -> OutpS ()
newDb !ts =
  S.get
    >>= ( \OutputState {..} ->
            if outEmpty
              then S.put OutputState {outEmpty = False, ..}
              else endLine
        )
    >> beginLine ts

outputNewEvent :: T.Text -> [EventData] -> OutpS ()
outputNewEvent !pname !dat =
  getOutHandle >>= (S.liftIO . (flip TIO.hPutStr) ev)
  where
    ev = " " +| pname |+ "(" +| (eventDataF dat) |+ ")"
    eventDataF = mconcat . (intersperse ", ") . (map build) . toList

beginLine ts =
  getOutHandle
    >>= (S.liftIO . (flip TIO.hPutStr) ("@" +| ts |+ " "))

endLine =
  getOutHandle
    >>= (S.liftIO . (flip TIO.hPutStr) ";\n")

getOutHandle = outHandle <$> S.get
