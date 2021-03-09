from sdg.inputs import InputBase
from mysql.connector import connect
import yaml
import os

class InputNsdpDatabase(InputBase):
    """Sources of SDG data/metadata from the NSDP MySQL database."""

    def execute(self, indicator_options):
        # Connect to the database.
        mydb = connect(user=os.environ.get('DBUSER'), password=os.environ.get('DBPASS'),
                       host=os.environ.get('DBHOST'), database=os.environ.get('DBNAME'))
        cursor = mydb.cursor(dictionary=True)

        cursor.execute(self.get_metadata_sql())
        data_sets = {}
        metadata_sets = {}
        source_sets = {}
        series_translations = {}

        for metadata_set in cursor.fetchall():

            indicator_id = metadata_set['NSDPIndicatorID']

            if indicator_id not in metadata_sets:
                metadata_sets[indicator_id] = metadata_set
            if indicator_id not in data_sets:
                data_sets[indicator_id] = []
            if indicator_id not in source_sets:
                source_sets[indicator_id] = []

            series_name = metadata_set['IndicatorShortName']
            data_id = metadata_set['DataID']
            series_id = str(int(data_id))

            series_translations[series_id] = series_name

            cursor.execute(self.get_value_sql(), [data_id])
            data_rows = cursor.fetchall()
            data_sets[indicator_id] += [{
                'Year': self.fix_year(r['Year']),
                'Units': self.fix_units(r['Value']),
                'Series': series_id,
                'Value': self.fix_value(r['Value']),
            } for r in data_rows]
            source_sets[indicator_id] += [r['Source'] for r in data_rows]

        for indicator_id in metadata_sets:
            cursor.execute(self.get_indicator_sql(), [indicator_id])
            indicator = cursor.fetchall()
            indicator = indicator[0]
            name = self.fix_indicator_name(indicator['NSDPIndicator'], indicator['NSDPIndicatorCode'])
            open_sdg_id = self.fix_indicator_id(indicator_id)

            data = None
            if len(data_sets[indicator_id]) > 0:
                data = self.create_dataframe(data_sets[indicator_id])

            # Calculate some settings.
            meta = metadata_sets[indicator_id]
            meta['indicator_number'] = open_sdg_id
            meta['goal_number'] = open_sdg_id.split('-')[0]
            meta['goal_name'] = 'global_goals.' + meta['goal_number'] + '-title'
            meta['target_number'] = open_sdg_id.split('-')[0] + '-' + open_sdg_id.split('-')[1]
            meta['target_name'] = 'global_targets.' + meta['target_number'] + '-title'
            meta['indicator_name'] = 'indicators.' + open_sdg_id + '-title'
            meta['graph_type'] = 'bar'
            meta['national_geographical_coverage'] = 'Vanuatu'
            meta['computation_units'] = meta['UnitofMeasure'] if 'UnitofMeasure' in meta else None

            sources = set(source_sets[indicator_id])
            num = 1
            for source in sources:
                meta['source_active_' + str(num)] = True
                meta['source_organisation_' + str(num)] = source
                num += 1

            # Create the indicator.
            self.add_indicator(open_sdg_id, data=data, meta=meta, name=name, options=indicator_options)

        #with open(os.path.join('translations', 'en', 'Series.yml'), 'w') as stream:
        #    yaml.dump(series_translations, stream)

    def get_indicator_sql(self):
        return "SELECT * FROM nsdpindicator WHERE NSDPIndicatorCode = %s"

    def get_value_sql(self):
        return "SELECT Year, Value, Source FROM nsdpyearvalue WHERE Data_ID = %s AND Value != 'NA'"

    def get_metadata_sql(self):
        return "SELECT * FROM nsdpmetadata"

    def fix_year(self, year):
        return int(year)

    def fix_value(self, value):
        value = value.replace('%', '')
        value = value.replace(',', '')
        value = value.replace('VUV', '')
        value = value.strip()
        if '/' in value:
            parts = value.split('/')
            return float(parts[0]) / float(parts[1])
        try:
            return int(value)
        except:
            try:
                return float(value)
            except:
                print('Unable to interpret value: ' + str(value))
        return 0

    def fix_indicator_id(self, indicator_id):
        return indicator_id.replace(' ', '').replace('.', '-')

    def fix_units(self, value):
        if '%' in value:
            return 'Percent'
        if 'VUV' in value:
            return 'VUV'
        return 'Total'

    def fix_indicator_name(self, name, code):
        return name.replace(code, '').strip().strip('.')
