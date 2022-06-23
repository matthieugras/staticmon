module RandomTestRunner (runRandomTests) where

import Control.Concurrent.Async.Lifted qualified as Async
import Control.Concurrent.QSem.Lifted
import Control.Exception.Lifted (bracket_, onException)
import Control.Monad (forM_, forever, replicateM)
import Control.Monad.Reader (ReaderT)
import Control.Monad.Reader qualified as RD
import Data.Function ((&))
import Data.Functor ((<&>))
import Data.Text qualified as T
import Data.Text.IO qualified as T
import EventGenerators (generateRandomLog)
import Flags (Flags (..), NestedFlags (..))
import Monitors (staticmon, verifyMonitor)
import Shelly.Helpers (FlagSh, shellyWithFlags)
import Shelly.Lifted
import SignatureParser (parseSig)

withGenerateFormula c =
  withTmpDir $ \d ->
    let f = d </> "out.mfotl"
        s = d </> "out.sig"
     in do
          print_stdout False $ (run_ "gen_fma" ["-output"])
          c f s

saveFiles s f l = do
  basedir <- f_mon_path <$> RD.ask
  forM_ [s, f, l] ((flip cp) basedir)

runSingleTest sem =
  (flip onException) (signalQSem sem) $
    withGenerateFormula $ \f s -> withTmpDir $ \d ->
      let l = d </> "log"
       in T.readFile s
            & liftIO
            <&> parseSig
            >>= generateRandomLog l
            >> onException (verifyMonitor staticmon (s, f, l)) (saveFiles s f l)

dispatchSingleTest sem =
  waitQSem sem
    >> onException (Async.async $ runSingleTest sem) (signalQSem sem)
    >> return ()

runRandomTests' =
  newQSem 16
    >>= forever dispatchSingleTest

runRandomTests :: Flags -> IO ()
runRandomTests =
  shelly
    . tracing False
    . shellyWithFlags runRandomTests'
