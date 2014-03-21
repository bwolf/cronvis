-- cronvis.hs my 1st test in haskell to translate transform.py to haskell
import Text.Regex
import Data.Char (isSpace)
import Data.List

import System.Locale (defaultTimeLocale)
import Data.Time
import Data.Time.Clock
import Data.Time.Clock.POSIX

import qualified Data.Map as Map

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
simplifyCmd = removeAbsolutePath. stringTrimmedUpToChar '>' . stringTrimmedUpToChar ' ' .
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
-- parseCronSyslog dateParser lines = map (parseCronLine dateParser) cronLines
parseCronSyslog dateParser lines = noEmpty $ parseIt
    where cronLines = filter (isInfixOf "/USR/SBIN/CRON") lines
          parseIt = map (parseCronLine dateParser) cronLines
          noEmpty = filter (not . null . snd)

-- Create list of UTCTimes in interval [start end[.
-- take into account that the jobs are ordered by using takeWhile instead of filter
between :: UTCTime -> UTCTime -> [CronjobExecution] -> [CronjobExecution]
between start end = takeWhile (pred . fst)
    where pred t = t >= start && t < end

-- Map of jobnames and their execution times in UTCTime
mapByJobnames :: [CronjobExecution] -> Map.Map String [UTCTime]
mapByJobnames jobs = Map.fromListWith (++) $ map (\(ts,name) -> (name,[ts])) jobs

-- Range of UTCTimes
timeRange :: UTCTime -> UTCTime -> Int -> [UTCTime]
timeRange start end stepSecs = iter s
    where s = floor (utcTimeToPOSIXSeconds start) :: Int
          e = floor (utcTimeToPOSIXSeconds end) :: Int
          iter n
              | n > e = []
              | otherwise = current : iter (n + stepSecs)
              where current = posixSecondsToUTCTime (fromIntegral n :: NominalDiffTime)

-- Simplified ISO8601 date format without seconds
formatMinuteTimestampForOutput :: UTCTime -> String
formatMinuteTimestampForOutput = formatTime defaultTimeLocale "%Y-%m-%dT%H:%M"

-- Full ISO8601 date timestamp
formatTimestampForOutput :: UTCTime -> String
formatTimestampForOutput = formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S"

-- Get just the current year
getCurrentYear :: IO Integer
getCurrentYear = do
  curr <- getCurrentTime
  let (y,_,_) = toGregorian $ utctDay curr
  return y

-- For command line timestamp parsing
parseIso8601Timestamp :: String -> UTCTime
parseIso8601Timestamp s = readTime defaultTimeLocale "%Y-%m-%dT%H:%M" s :: UTCTime

-- Full timestamp each N minutes
csvColumnTimestamps :: UTCTime -> UTCTime -> Int -> String
csvColumnTimestamps start end stepSecs = intercalate filler timestamps
    where fullTimestampSecs = 60
          filler     = concat $ replicate fullTimestampSecs ";"
          range      = (timeRange start end (stepSecs * fullTimestampSecs ))
          timestamps = map formatMinuteTimestampForOutput range

csvColumnMinutes :: UTCTime -> UTCTime -> Int -> String
csvColumnMinutes start end stepSecs = concatMap (\ut -> show(minute ut) ++ ";") range
    where minute ut = todMin (timeToTimeOfDay $ utctDayTime ut)
          range     = timeRange start end stepSecs

csvData :: UTCTime -> UTCTime -> Int -> Map.Map String [UTCTime] -> String
csvData start end stepSecs jobnamesMap = concatMap printHelper jobnames
    where jobnames          = sort (Map.keys jobnamesMap)
          pointPri times ut = if ut `elem` times
                              then "R;"
                              else ";"
          printHelper job   =
              let times = Map.lookup job jobnamesMap
              in job ++ case times of
                          Just times' -> ";" ++ concatMap (pointPri times')
                                         (timeRange start end stepSecs) ++ "\n"
                          Nothing     -> error "Failed to get times of job; invariant failed"

csvExport :: UTCTime -> UTCTime -> Map.Map String [UTCTime] -> String
csvExport start end jobnamesMap = csvColumnTimestamps start end stepSecs ++
                                  csvColumnMinutes start end stepSecs ++
                                  csvData start end stepSecs jobnamesMap
    where stepSecs = 60

csvExport2 :: UTCTime -> UTCTime -> Map.Map String [UTCTime] -> String
csvExport2 start end jobnamesMap =
    "Jobname/Timestamp;" ++ csvColumnTimestamps start end stepSecs ++ "\n" ++
    "Minutes;" ++ csvColumnMinutes start end stepSecs ++ "\n" ++
    csvData start end stepSecs jobnamesMap
        where stepSecs = 60

s = parseIso8601Timestamp "2014-03-17T06:25"
e = parseIso8601Timestamp "2014-03-17T07:26"

foo = do
  contents <- readFile "syslog-20140318"
  let logLines = lines contents
  year <- getCurrentYear
  let dateParser = parseCronTimestamp year
  let database = parseCronSyslog dateParser logLines
  let jobsBetween = between s e database
  putStrLn $ "Got " ++ show(length jobsBetween) ++ " jobs between start,end"
  let jobnamesMap = mapByJobnames database
  -- return jobnamesMap
  putStrLn $ csvExport2 s e jobnamesMap

-- main = do
--   contents <- readFile "syslog-20140318"
--   let logLines = lines contents
--   putStrLn $ "Got " ++ show(length logLines) ++ " lines"
--   year <- getCurrentYear
--   putStrLn $ "Hello the current year is " ++ show year
--   let dateParser = parseCronTimestamp year
--   let database = parseCronSyslog dateParser logLines
--   putStrLn $ "Got " ++ show(length database) ++ " cron entries"
--   let jobsBetween = between s e database
--   putStrLn $ "Got " ++ show(length jobsBetween) ++ " jobs between start,end"
--   let jobnamesMap = mapByJobnames database
--   putStrLn $ csvExport s e jobnamesMap

-- EOF
