-- cronvis.hs by MG on 2014-03-22: create syslog execution analysis as
-- CSV file from syslog logfile.

import Text.Regex
import Data.Char (isSpace)
import Data.List

import System.Locale (defaultTimeLocale)
import Data.Time
import Data.Time.Clock.POSIX

import qualified Data.Map as Map
import qualified Data.Set as Set

import System.Environment (getArgs, getProgName)
import System.IO (stderr, hPutStrLn)
import System.Exit (exitFailure)

import Control.Monad

trim :: String -> String
trim = f . f
    where f = reverse . dropWhile isSpace

removeNiceCmd :: String -> String
removeNiceCmd s = subRegex re s ""
    where re = mkRegex "/usr/bin/nice( -n [[:digit:]]+)?[[:blank:]]*"

removeCronStartCmd :: String -> String
removeCronStartCmd s = subRegex re s ""
    where re = mkRegex "/usr/projekt/tools/bin/cron.start2?[[:blank:]]*"

stringTrimmedUpToChar :: Char -> String -> String
stringTrimmedUpToChar c s = trim s'
    where s' = takeWhile (/= c) s

removeAbsolutePath :: String -> String
removeAbsolutePath s = reverse $ takeWhile (/= '/') $ reverse s

simplifyCmd :: String -> String
simplifyCmd = removeAbsolutePath . stringTrimmedUpToChar '>' . stringTrimmedUpToChar ' ' .
              removeNiceCmd . removeCronStartCmd . stringTrimmedUpToChar '#'

extractCronJob :: String -> String
extractCronJob s =
    case match of
      Just value -> head value -- get first grouped match
      Nothing    -> "" -- maybe a cron error log line
    where match   = matchRegex cronCmd s
          cronCmd = mkRegex "^.*CMD \\((.*)\\)[[:blank:]]*$"

-- Convert a cron timestamp (which is missing the year) to a UTCTime with the current year
parseCronTimestamp :: Integer -> String -> UTCTime
parseCronTimestamp currentYear str = UTCTime correctedDay $ utctDayTime time
    where time         = readTime defaultTimeLocale "%b %d %H:%M" str :: UTCTime
          yearDelta    = currentYear - 1970
          correctedDay = addGregorianYearsClip yearDelta $ utctDay time

-- A type for a cronjob and its execution timestamp
type CronjobExecution = (UTCTime, String)
type CronDateParser = (String -> UTCTime)

parseCronLine :: CronDateParser -> String -> CronjobExecution
parseCronLine dateParser s = (time, jobname)
    where time    = dateParser $ take 12 s -- take date without seconds
          jobname = simplifyCmd $ extractCronJob s

-- Filter and parse all cron commands out of syslog log lines
parseCronSyslog :: CronDateParser -> [String] -> [CronjobExecution]
parseCronSyslog dateParser logLines = noEmpty parseIt
    where cronLines = filter (isInfixOf "/USR/SBIN/CRON") logLines
          parseIt = map (parseCronLine dateParser) cronLines
          noEmpty = filter (not . null . snd)

-- A type for cronjobs mapped by name to a set of the execution times
type CronjobExecutionMap = Map.Map String (Set.Set UTCTime)

mkCronjobExecutionMap :: [CronjobExecution] -> CronjobExecutionMap
mkCronjobExecutionMap jobs = Map.fromListWith Set.union $
                             map (\(ts,name) -> (name, Set.singleton ts)) jobs

-- A range of UTCTimes
type TimeRange = [UTCTime]

timeRange :: UTCTime -> UTCTime -> Int -> TimeRange
timeRange start end stepSecs = iter s
    where s = floor (utcTimeToPOSIXSeconds start) :: Int
          e = floor (utcTimeToPOSIXSeconds end) :: Int
          iter n | n > e = []
                 | otherwise = curr : iter (n + stepSecs)
              where curr = posixSecondsToUTCTime (fromIntegral n :: NominalDiffTime)

-- Simplified ISO8601 date format without seconds
formatMinuteTimestampForOutput :: UTCTime -> String
formatMinuteTimestampForOutput = formatTime defaultTimeLocale "%Y-%m-%dT%H:%M"

-- Full timestamp each N minutes
csvColumnTimestamps :: UTCTime -> UTCTime -> Int -> String
csvColumnTimestamps start end stepSecs = intercalate filler timestamps
    where fullTimestampSecs = 60
          filler     = concat $ replicate fullTimestampSecs ";"
          range      = timeRange start end (stepSecs * fullTimestampSecs)
          timestamps = map formatMinuteTimestampForOutput range

csvColumnMinutes :: TimeRange -> String
csvColumnMinutes = concatMap (\ut -> show(minute ut) ++ ";")
    where minute ut = todMin (timeToTimeOfDay $ utctDayTime ut)

csvData :: TimeRange -> CronjobExecutionMap -> String
csvData tRange jobMap = concatMap printer (sort $ Map.keys jobMap)
    where pointPri times ut | ut `Set.member` times = "R;"
                            | otherwise             = ";"
          printer job   =
              let times = Map.lookup job jobMap
              in job ++ case times of
                          Just t  -> ";" ++ concatMap (pointPri t) tRange ++ "\n"
                          Nothing -> error "No exec times for job; invariant failed"

csvExport :: UTCTime -> UTCTime -> CronjobExecutionMap -> String
csvExport start end jobnamesMap =
    let range = timeRange start end 60
        stepSecs = 60
    in "Jobname/Timestamp;" ++ csvColumnTimestamps start end stepSecs ++ "\n" ++
       "Minutes;" ++ csvColumnMinutes range ++ "\n" ++
       csvData range jobnamesMap

-- Get just the current year
getCurrentYear :: IO Integer
getCurrentYear = do
  curr <- getCurrentTime
  let (y,_,_) = toGregorian $ utctDay curr
  return y

-- For command line timestamp parsing
parseIso8601Timestamp :: String -> UTCTime
parseIso8601Timestamp s = readTime defaultTimeLocale "%Y-%m-%dT%H:%M" s :: UTCTime

-- Example for ghci :main syslog-20140318 2014-03-17T06:25 2014-03-17T07:26
main :: IO ()
main = do
  me <- getProgName
  args <- getArgs
  when (3 /= length args) $ do
    hPutStrLn stderr $ "Usage: " ++ me ++ " file startDate endDate\n" ++
              "  where startdate,endDate is a ISO8601 timestamp w/o seconds.\n"
    exitFailure

  let (fname:s:e:_) = args
  let start = parseIso8601Timestamp s
      end = parseIso8601Timestamp e
  contents <- readFile fname
  year <- getCurrentYear
  let dat1 = parseCronSyslog (parseCronTimestamp year) (lines contents)
      start' = max start (fst $ head dat1)
      end' = min end (fst $ last dat1)
  putStrLn $ csvExport start' end' (mkCronjobExecutionMap dat1)

-- EOF
