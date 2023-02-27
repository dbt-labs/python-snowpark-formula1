with int_results as (

  select * from {{ ref('int_results') }}

),

int_pit_stops as (
  select 
    race_id,
    driver_id, 
    max(total_pit_stops_per_race) as total_pit_stops_per_race
  from {{ ref('int_pit_stops') }}
  group by 1,2
  
), 

circuits as (

  select * from {{ ref('stg_f1_circuits') }}
  
), 
 
 base_results as (
     select 
        result_id,
        int_results.race_id,
        race_year, 
        race_round,
        int_results.circuit_id,
        int_results.circuit_name,
        circuit_ref,
        location,
        country,
        latitude,
        longitude,
        altitude,
        total_pit_stops_per_race, 
        race_date,
        race_time, 
        int_results.driver_id, 
        driver, 
        driver_number,
        drivers_age_years, 
        driver_nationality,
        constructor_id,
        constructor_name,
        constructor_nationality, 
        grid, 
        position,
        position_text,
        position_order,
        points,
        laps,
        results_time_formatted, 
        results_milliseconds,
        fastest_lap,
        results_rank,
        fastest_lap_time_formatted,
        fastest_lap_speed, 
        status_id,
        status,
        dnf_flag
     from int_results
     left join circuits
        on int_results.circuit_id=circuits.circuit_id
     left join int_pit_stops 
        on int_results.driver_id=int_pit_stops.driver_id and int_results.race_id=int_pit_stops.race_id
 )

select * from base_results 