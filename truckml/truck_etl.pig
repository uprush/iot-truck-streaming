register pigudf.jar

drivers = LOAD 'drivers' USING org.apache.hive.hcatalog.pig.HCatLoader();
timesheet = LOAD 'timesheet' USING org.apache.hive.hcatalog.pig.HCatLoader();
events = LOAD '/truck-events-v4/staging' using PigStorage(',') as (driverid:int,truckid:int,event_time:chararray,event_type:chararray,longitude:double,latitude:double,eventkey:chararray,correlationid:long,driver_name:chararray,route_id:int,route_name:chararray);
events_cleaned = FILTER events by (driverid is not null) AND (truckid is not null) AND (event_time is not null) AND (event_type is not null) AND (longitude is not null) AND (latitude is not null) AND (eventkey is not null) AND (correlationid is not null) AND (driver_name is not null) AND (route_id is not null) AND (route_name is not null);

/* extract normal & violation events */
n_events = FILTER events_cleaned by event_type == 'Normal';
v_events = FILTER events_cleaned by event_type != 'Normal';

/* take equal samples of violation & normal event classes for training ML algo. */
events_s = UNION (LIMIT v_events 1200), (LIMIT n_events 1200);

/* convert the timestamps on events to day of week */
events_s = foreach events_s generate driverid, event_type, GetWeek(ToDate(SUBSTRING(event_time,0,LAST_INDEX_OF(event_time,'.')),'yyyy-MM-dd HH:mm:SS')) AS week, longitude,latitude, event_time;


/* enrich events with driver data from hcat */
events_d = JOIN drivers BY driverid, events_s BY driverid;


/* enrich events with timesheet data from hcat */
events_j_timesheet = join timesheet by (driverid, week), events_d by (drivers::driverid, events_s::week);


/* transform enriched events for exploration with tableau */ 
tableau = foreach events_j_timesheet generate events_s::event_type, drivers::certified, drivers::wage_plan, timesheet::hours_logged, timesheet::miles_logged, longitude, latitude, weatherudf.GetFog(events_s::event_type,longitude,latitude,event_time) as foggy, weatherudf.GetRainStorm(events_s::event_type,longitude,latitude,event_time) as rainy, weatherudf.GetWindy(events_s::event_type,longitude,latitude,event_time) as windy;

/* remove old tableau visualization data from HDFS */
rmf /user/root/truck_tableau/

STORE tableau INTO '/user/root/truck_tableau/' USING PigStorage(',');

/* transform enriched events for training spark ML algo. */
training_sparkML = foreach events_j_timesheet generate (events_s::event_type!='Normal'?'1,':'0,') as class, (drivers::certified=='Y'?'1':'0') as f1, (drivers::wage_plan=='miles'?'1':'0') as f2, (((double)timesheet::hours_logged)/100) as f3, (((double)timesheet::miles_logged)/10000) as f4, weatherudf.GetFog(events_s::event_type,longitude,latitude,event_time) as f5, weatherudf.GetRainStorm(events_s::event_type,longitude,latitude,event_time) as f6, weatherudf.GetWindy(events_s::event_type,longitude,latitude,event_time) as f7;


/* remove old training data from HDFS */
rmf /user/root/truck_training/

/* write spark ML training data to HDFS */
STORE training_sparkML INTO '/user/root/truck_training/' USING PigStorage(' ');

