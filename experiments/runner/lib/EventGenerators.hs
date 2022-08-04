{-# LANGUAGE ParallelListComp #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Use second" #-}

module EventGenerators (getGenerator, generateRandomLog) where

import Control.Monad (forM_, replicateM, replicateM_, when)
import Control.Monad.Reader (ReaderT)
import Control.Monad.Reader qualified as R
import Data.Int (Int64)
import Data.Random.Distribution.Categorical (categorical)
import Data.Random.RVar (runRVar)
import Data.Text qualified as T
import EventPrinting
import Flags (Flags (..), NestedFlags (..))
import SignatureParser (SigType (..), Signature)
import System.Random.Stateful (globalStdGen, uniformRM)

intArgs = map Intgr

getGenerator :: T.Text -> (FilePath -> IO ())
getGenerator genid =
  case genid of
    "simple_pred_1" -> simplePred1Gen
    "rel_ops1" -> relOps1Gen
    "fused_simp_ops_1" -> fusedSimpleOps1Gen
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

-- EXISTS y. (A(x, y) AND (x >= 1) AND (x < 2) AND (z = y))
fusedSimpleOps1Gen :: FilePath -> IO ()
fusedSimpleOps1Gen fp =
  let rvar = categorical [(0.495, 0), (0.495, 1), (0.01, 2)]
   in withPrintState fp $ do
        forM_ [0 .. 1000000] $ \i -> do
          yval <- uniformRM (0 :: Int64, 100000) globalStdGen
          newDb i
          xval <-
            runRVar rvar globalStdGen
              >>= \case
                0 ->
                  uniformRM (-1000, 0) globalStdGen
                1 ->
                  uniformRM (11, 10000) globalStdGen
                2 ->
                  uniformRM (1, 10) globalStdGen
                _ -> error "cannot happen"
          outputNewEvent "A" (intArgs [xval, yval])
        endOutput

randomEvent arity ub =
  intArgs <$> replicateM arity (uniformRM (0, ub) globalStdGen)

generateRandomLog :: FilePath -> Signature -> ReaderT Flags IO ()
generateRandomLog fp sig =
  let arrsig = map (\(a, b) -> (a, length b)) sig
   in do
        when (any (any (/= IntTy) . snd) sig) $
          error "only integer signatures supported"
        Flags {f_nes_flags = RandomTestFlags {..}, ..} <- R.ask
        R.liftIO . withPrintState fp $ do
          forM_ [0 .. rt_num_ts] $ \ts ->
            replicateM_ rt_db_per_ts $ do
              newDb (fromIntegral ts)
              forM_ arrsig $ \(name, arity) ->
                replicateM_ rt_events_per_db $
                  randomEvent arity rt_ub
                    >>= outputNewEvent name
          endOutput
