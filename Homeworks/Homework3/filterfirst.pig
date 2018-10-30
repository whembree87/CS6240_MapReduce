REGISTER ../trunk/contrib/piggybank/java/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

------------------------------------------------------------------------------------------------------------------
-- Load data
------------------------------------------------------------------------------------------------------------------

flights1 = LOAD './input/testData.csv' USING CSVLoader() AS
(year: int,
quarter: int,
month: int,
dayOfMonth: int,
dayOfWeek: int,
flightDate: datetime,
uniqueCarrier: chararray,
airlineID: int,
carrier: chararray,
tailNum: chararray,
flightNum: int,
origin: chararray,
originCityName: chararray,
originState: chararray,
originStateFips: chararray,
originStateName: chararray,
originWac: int,
dest: chararray,
destCityName: chararray,
destState: chararray,
destStateFips: chararray,
destStateName: chararray,
destWac: int,
crsDepTime: chararray,
depTime: int,
depDelay: int,
depDelayMinutes: int,
depDell5: int,
departureDelayGroups: int,
depTimeBlk: datetime,
taxiOut: int,
wheelsOff: chararray,
wheelsOn: chararray,
taxiIn: int,
crsArrTime: chararray,
arrTime: int,
arrDelay: int,
arrDelayMinutes: int,
arrDell15: int,
arrivalDelayGroups: int,
arrTimeBlk: datetime,
cancelled: int,
cancellationCode: chararray,
diverted: int,
crsElapsedTime: int,
actualElapsedTime: int,
airTime: int,
flights: int,
distance: int,
distanceGroup: int,
carrierDelay: int,
weatherDelay: int,
nasDelay: int,
securityDelay: int,
lateAircraftDelay: int
);

flights2 = LOAD './input/testData.csv' USING CSVLoader() AS
(year: int,
quarter: int,
month: int,
dayOfMonth: int,
dayOfWeek: int,
flightDate: datetime,
uniqueCarrier: chararray,
airlineID: int,
carrier: chararray,
tailNum: chararray,
flightNum: int,
origin: chararray,
originCityName: chararray,
originState: chararray,
originStateFips: chararray,
originStateName: chararray,
originWac: int,
dest: chararray,
destCityName: chararray,
destState: chararray,
destStateFips: chararray,
destStateName: chararray,
destWac: int,
crsDepTime: chararray,
depTime: int,
depDelay: int,
depDelayMinutes: int,
depDell5: int,
departureDelayGroups: int,
depTimeBlk: datetime,
taxiOut: int,
wheelsOff: chararray,
wheelsOn: chararray,
taxiIn: int,
crsArrTime: chararray,
arrTime: int,
arrDelay: int,
arrDelayMinutes: int,
arrDell15: int,
arrivalDelayGroups: int,
arrTimeBlk: datetime,
cancelled: int,
cancellationCode: chararray,
diverted: int,
crsElapsedTime: int,
actualElapsedTime: int,
airTime: int,
flights: int,
distance: int,
distanceGroup: int,
carrierDelay: int,
weatherDelay: int,
nasDelay: int,
securityDelay: int,
lateAircraftDelay: int
);

------------------------------------------------------------------------------------------------------------------
-- Filter out "bad" flights
------------------------------------------------------------------------------------------------------------------

-- Neither cancelled nor diverted

active_flights1 = FILTER flights1 BY (cancelled != 1) AND (diverted != 1);
active_flights2 = FILTER flights2 BY (cancelled != 1) AND (diverted != 1);

-- No direct (ORD ---> JFK) trips

potential_two_leg_flights1 = FILTER active_flights1 BY ((origin != 'ORD') OR (dest != 'JFK'));
potential_two_leg_flights2 = FILTER active_flights2 BY ((origin != 'ORD') OR (dest != 'JFK'));

------------------------------------------------------------------------------------------------------------------
-- Define flights originating from ORD (f1) versus flights destined for JFK (f2)
------------------------------------------------------------------------------------------------------------------

f1_flights = FILTER potential_two_leg_flights1 BY (origin == 'ORD');

f2_flights = FILTER potential_two_leg_flights2 BY (dest == 'JFK');

------------------------------------------------------------------------------------------------------------------
-- Join on destination X
-- Chicago (ORD) ----F1----> Destination X (key) ----F2----> New York (JFK)
------------------------------------------------------------------------------------------------------------------

-- intermed_dest_grouped_flights = JOIN f1_flights BY (dest, flightDate), f2_flights BY (origin, flightDate);

------------------------------------------------------------------------------------------------------------------
-- Filter where F2 Depart Time > F1 Arrival Time
------------------------------------------------------------------------------------------------------------------

-- flights_punctual_dep_arr_t = FILTER intermed_dest_grouped_flights BY (f2_flights::depTime > f1_flights::arrTime);

------------------------------------------------------------------------------------------------------------------
-- Date range June 2007 to May 2008
-- NOTE : Check that the flight date of ONLY f1_flights is in range
------------------------------------------------------------------------------------------------------------------

-- flights_in_date_range = FILTER flights_punctual_dep_arr_t BY (
(f1_flights::flightDate >= ToDate('2007-10-02')) AND (f1_flights::flightDate <= ToDate('2007-10-05')));

------------------------------------------------------------------------------------------------------------------
-- Average delay
------------------------------------------------------------------------------------------------------------------

-- grouped_two_leg_flights = GROUP flights_in_date_range ALL;
-- total_flights = FOREACH grouped_two_leg_flights GENERATE COUNT(flights_in_date_range);

-- flight_delays = FOREACH flights_in_date_range GENERATE (f1_flights::arrDelayMinutes  + f2_flights::arrDelayMinutes);
-- total_delays = GROUP flight_delays ALL;
-- total_delay = FOREACH total_delays GENERATE total_flights.$0, SUM(flight_delays);

-- total_avg_delay = FOREACH total_delay GENERATE (total_delay.$1/total_delay.$0);

------------------------------------------------------------------------------------------------------------------
-- Output result
------------------------------------------------------------------------------------------------------------------

store f1_flights into './output/';


