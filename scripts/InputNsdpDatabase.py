from sdg.inputs import InputBase
from mysql.connector import connect
import os

class InputNsdpDatabase(InputBase):
    """Sources of SDG data/metadata from the NSDP MySQL database."""

    def execute(self, indicator_options):
        # Connect to the database.
        mydb = connect(user=os.environ.get('DBUSER'), password=os.environ.get('DBPASS'),
                       host=os.environ.get('DBHOST'), database=os.environ.get('DBNAME'))
        cursor = mydb.cursor(dictionary=True)

        # Query the indicators.
        cursor.execute(self.get_indicator_sql())
        for indicator in cursor.fetchall():
            indicator_id = indicator['NSDPIndicatorCode']
            open_sdg_id = self.fix_indicator_id(indicator_id)
            name = self.fix_indicator_name(indicator['NSDPIndicator'], indicator['NSDPIndicatorCode'])

            # Query the data.
            cursor.execute(self.get_value_sql(), [indicator_id])
            data_rows = cursor.fetchall()
            rows = [{
                'Year': self.fix_year(r['Year']),
                'Units': self.fix_units(r['Value']),
                'Series': self.fix_series(r['Proxy'], open_sdg_id),
                'Value': self.fix_value(r['Value']),
            } for r in data_rows]

            data = None
            if len(rows) > 0:
                data = self.create_dataframe(rows)

            # Query the metadata.
            cursor.execute(self.get_metadata_sql(), [indicator_id])
            meta = cursor.fetchall()
            # TODO: How to handle multiple metadata sets per indicator?
            #       For now just use the first one.
            meta = meta[0] if len(meta) > 0 else {}
            # Calculate some settings.
            meta['indicator_number'] = open_sdg_id
            meta['goal_number'] = open_sdg_id.split('-')[0]
            meta['goal_name'] = 'global_goals.' + meta['goal_number'] + '-title'
            meta['target_number'] = open_sdg_id.split('-')[0] + '-' + open_sdg_id.split('-')[1]
            meta['target_name'] = 'global_targets.' + meta['target_number'] + '-title'
            meta['indicator_name'] = 'indicators.' + open_sdg_id + '-title'
            meta['graph_type'] = 'bar'
            meta['national_geographical_coverage'] = 'Vanuatu'
            meta['computation_units'] = meta['UnitofMeasure'] if 'UnitofMeasure' in meta else None

            sources = set([r['Source'] for r in data_rows])
            num = 1
            for source in sources:
                meta['source_active_' + str(num)] = True
                meta['source_organisation_' + str(num)] = source
                num += 1

            # Create the indicator.
            self.add_indicator(open_sdg_id, data=data, meta=meta, name=name, options=indicator_options)

    def get_indicator_sql(self):
        return "SELECT * FROM nsdpindicator"

    def get_value_sql(self):
        return "SELECT r.Year, r.Value, r.Source, d.Proxy FROM nsdpdata AS d JOIN nsdpyearvalue AS r ON d.DataID = r.Data_ID WHERE d.IndicatorID = %s AND r.Value != 'NA'"

    def get_metadata_sql(self):
        return "SELECT * FROM nsdpmetadata WHERE NSDPIndicatorID = %s"

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

    def fix_series(self, proxy, indicator_id):
        if proxy:
            return 'PROXY-' + indicator_id
        return ''

    def fix_indicator_name(self, name, code):
        return name.replace(code, '').strip().strip('.')
