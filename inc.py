from locallib.pandas.Timezone import *
from locallib.pandas.EmissionRates import *
def format_emission_sources(emission_sources):

    #Add LisaNumber column by extracting the last number from UniqueIdentifier
    emission_sources['LisaNumber'] = emission_sources['UniqueIdentifier'].str.extract(r'-(\d+)$')[0]
    
    #Add the ReportDateLocal column
    emission_sources = emission_sources.timezone.convert_utc_column_to_local('ReportDate', 'CommonTimeZone', 'ReportDateLocal')
    
    emission_sources.sort_values(by=['ReportId','RepresentativeBinLabel'],inplace=True,ascending=False)
    emission_sources['UniqueBoundary'] = (~emission_sources.duplicated(subset='ReportId', keep='first')).astype(int)

    # UniqueLISA: mark unique EmissionSourceId (first occurrence only) = 1
    emission_sources['UniqueLISA'] = (~emission_sources.duplicated(subset='EmissionSourceId', keep='first')).astype(int)

    # DuplicatedPeakInDifferentReports
    dup_mask = ~emission_sources[["ReportId", "RepresentativePeakId"]].duplicated()

    dup_mask &= emission_sources.drop_duplicates(subset=["ReportId", "RepresentativePeakId"]) \
                .duplicated(subset="RepresentativePeakId", keep=False) \
                .fillna(True)

    emission_sources['DuplicatedPeakInDifferentReports'] = dup_mask.astype(int)

    #Postprocess the emission sources
    emission_sources = emission_sources.emissionrates.convert()

    #Drop columns
    emission_sources = emission_sources.drop(columns=['CommonTimeZone'])

    #Reorder columns to place ReportDateLocal next to ReportDate
    cols = emission_sources.columns.tolist()
    if 'ReportDateLocal' in cols and 'ReportDate' in cols:
        # Remove ReportDateLocal from its current position
        cols.remove('ReportDateLocal')
        # Find ReportDate index and insert ReportDateLocal right after it
        report_date_idx = cols.index('ReportDate')
        cols.insert(report_date_idx + 1, 'ReportDateLocal')
        # Reorder the dataframe
        emission_sources = emission_sources[cols]
    #Reorder columns to place LisaNumber next to LisaWkt4326
    cols = emission_sources.columns.tolist()
    if 'LisaNumber' in cols and 'LisaWkt4326' in cols:
        # Remove LisaNumber from its current position
        cols.remove('LisaNumber')
        # Find LisaWkt4326 index and insert LisaNumber right after it
        lisa_wkt_idx = cols.index('LisaWkt4326')
        cols.insert(lisa_wkt_idx + 1, 'LisaNumber')
        # Reorder the dataframe
        emission_sources = emission_sources[cols]
    return emission_sources