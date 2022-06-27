module Shelly.Helpers (FlagSh (..), shellyWithFlags) where

import Control.Monad.Reader (ReaderT (..), runReader)
import Flags (Flags)
import Shelly.Lifted

type FlagSh a = ReaderT Flags Sh a

shellyWithFlags :: FlagSh a -> Flags -> Sh a
shellyWithFlags = runReaderT
