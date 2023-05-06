WITH constructor_standings AS (
    SELECT     
        constructor_standings_id    AS constructor_standings_id,
        race_id                     AS race_id,
        constructor_id              AS constructor_id,
        points                      AS points,
        constructor_position        AS constructor_position,
        position_text               AS position_text,
        wins                        AS wins
    FROM {{ ref('stg_constructor_standings') }}
)

SELECT * FROM constructor_standings