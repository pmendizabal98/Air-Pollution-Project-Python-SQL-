import pandas as pd

try:
    df = pd.read_csv("crop.csv", low_memory=False, delimiter=";")

    site_id_values = {
        188: 'AURN Bristol Centre',
        203: 'Brislington Depot',
        206: 'Rupert Street',
        209: 'IKEA M32',
        213: 'Old Market',
        215: 'Parson Street School',
        228: 'Temple Meads Station',
        270: 'Wells Road',
        271: 'Trailer Portway P&R',
        375: 'Newfoundland Road Police Station',
        395: "Shiner's Garage",
        452: 'AURN St Pauls',
        447: 'Bath Road',
        459: 'Cheltenham Road \ Station Road',
        463: 'Fishponds Road',
        481: 'CREATE Centre Roof',
        500: 'Temple Way',
        501: 'Colston Avenue',
        672: 'Marlborough Street'
    }

    # create a boolean mask for dud records
    mask = df['SiteID'].isna() | ~df['SiteID'].isin(site_id_values.keys()) | \
           (df['Location'] != df['SiteID'].map(site_id_values))

    # filter out dud records and store them separately
    dud_records = df[mask].copy()

    # print the line number and mismatch field values for each dud record
    for index, row in dud_records.iterrows():
        site_id = row.get('SiteID')
        location = row.get('Location')
        print(f"Line {index} - SiteID: {site_id}, Location: {location}")

    # drop the dud records from the original dataframe
    df.drop(index=dud_records.index, inplace=True)

    # write the clean dataframe to a CSV file
    df.to_csv("clean.csv", index=False)

except Exception as e:
    print(f"An error occurred: {e}")
