# the intent of this .md is to allow for multi-line long form explanations for our intermediate transformations

# below are descriptions 
{% docs int_results %} In this query we want to join out other important information about the race results to have a human readable table about results, races, drivers, constructors, and status. 
We will have 4 left joins onto our results table. {% enddocs %}

{% docs int_pit_stops %} There are many pit stops within one race, aka a M:1 relationship. 
We want to aggregate this so we can properly join pit stop information without creating a fanout.  {% enddocs %}

{% docs int_lap_times_years %} Lap times are done per lap. We need to join them out to the race year to understand yearly lap time trends. {% enddocs %}