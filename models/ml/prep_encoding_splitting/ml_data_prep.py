import pandas as pd

def model(dbt, session):
    # dbt configuration
    dbt.config(packages=["pandas==1.5.3"])

    # get upstream data
    fct_results = dbt.ref("mrt_results_circuits").to_pandas()

    # provide years so we do not hardcode dates in filter command
    start_year=2010
    end_year=2020

    # describe the data for a full decade
    data =  fct_results.loc[fct_results['RACE_YEAR'].between(start_year, end_year)]

    # convert string to an integer
    # data['POSITION'] = data['POSITION'].astype(float)

    # we cannot have nulls if we want to use total pit stops 
    data['TOTAL_PIT_STOPS_PER_RACE'] = data['TOTAL_PIT_STOPS_PER_RACE'].fillna(0)

    # some of the constructors changed their name over the year so replacing old names with current name
    mapping = {'Force India': 'Racing Point', 'Sauber': 'Alfa Romeo', 'Lotus F1': 'Renault', 'Toro Rosso': 'AlphaTauri'}
    data['CONSTRUCTOR_NAME'].replace(mapping, inplace=True)

    # create confidence metrics for drivers and constructors
    dnf_by_driver = data.groupby('DRIVER').sum()['DNF_FLAG']
    driver_race_entered = data.groupby('DRIVER').count()['DNF_FLAG']
    driver_dnf_ratio = (dnf_by_driver/driver_race_entered)
    driver_confidence = 1-driver_dnf_ratio
    driver_confidence_dict = dict(zip(driver_confidence.index,driver_confidence))

    dnf_by_constructor = data.groupby('CONSTRUCTOR_NAME').sum()['DNF_FLAG']
    constructor_race_entered = data.groupby('CONSTRUCTOR_NAME').count()['DNF_FLAG']
    constructor_dnf_ratio = (dnf_by_constructor/constructor_race_entered)
    constructor_relaiblity = 1-constructor_dnf_ratio
    constructor_relaiblity_dict = dict(zip(constructor_relaiblity.index,constructor_relaiblity))

    data['DRIVER_CONFIDENCE'] = data['DRIVER'].apply(lambda x:driver_confidence_dict[x])
    data['CONSTRUCTOR_RELAIBLITY'] = data['CONSTRUCTOR_NAME'].apply(lambda x:constructor_relaiblity_dict[x])

    #removing retired drivers and constructors
    active_constructors = ['Renault', 'Williams', 'McLaren', 'Ferrari', 'Mercedes',
                        'AlphaTauri', 'Racing Point', 'Alfa Romeo', 'Red Bull',
                        'Haas F1 Team']
    active_drivers = ['Daniel Ricciardo', 'Kevin Magnussen', 'Carlos Sainz',
                    'Valtteri Bottas', 'Lance Stroll', 'George Russell',
                    'Lando Norris', 'Sebastian Vettel', 'Kimi Räikkönen',
                    'Charles Leclerc', 'Lewis Hamilton', 'Daniil Kvyat',
                    'Max Verstappen', 'Pierre Gasly', 'Alexander Albon',
                    'Sergio Pérez', 'Esteban Ocon', 'Antonio Giovinazzi',
                    'Romain Grosjean','Nicholas Latifi']

    # create flags for active drivers and constructors so we can filter downstream              
    data['ACTIVE_DRIVER'] = data['DRIVER'].apply(lambda x: int(x in active_drivers))
    data['ACTIVE_CONSTRUCTOR'] = data['CONSTRUCTOR_NAME'].apply(lambda x: int(x in active_constructors))
    
    return data