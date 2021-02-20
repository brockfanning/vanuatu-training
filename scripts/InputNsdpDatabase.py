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
            name = indicator['NSDPIndicator']

            # Query the data.
            cursor.execute(self.get_value_sql(), [indicator_id])
            rows = [{ 'Year': self.fix_year(r['Year']), 'Value': self.fix_value(r['Value']) } for r in cursor.fetchall()]
            data = None
            if len(rows) > 0:
                data = self.create_dataframe(rows)

            # Query the metadata.
            cursor.execute(self.get_metadata_sql(), [indicator_id])
            #print(cursor.fetchall())
            meta = cursor.fetchall()
            # TODO: How to handle multiple metadata sets per indicator?
            #       For now just use the first one.
            meta = meta[0] if len(meta) > 0 else {}
            # Calculate some settings.
            open_sdg_id = self.fix_indicator_id(indicator_id)
            meta['indicator_number'] = open_sdg_id
            meta['goal_number'] = open_sdg_id.split('-')[0]
            meta['target_number'] = open_sdg_id.split('-')[0] + '-' + open_sdg_id.split('-')[1]
            meta['graph_type'] = 'bar'
            meta['national_geographical_coverage'] = 'Vanuatu'
            meta['computation_units'] = meta['UnitofMeasure'] if 'UnitofMeasure' in meta else None

            # Create the indicator.
            self.add_indicator(open_sdg_id, data=data, meta=meta, name=name, options=indicator_options)

    def get_indicator_sql(self):
        return "SELECT * FROM nsdpindicator"

    def get_value_sql(self):
        return "SELECT r.Year, r.Value FROM nsdpdata AS d JOIN nsdpyearvalue AS r ON d.RecordID = r.RecordID WHERE d.IndicatorID = %s AND r.Value != 'NA'"

    def get_metadata_sql(self):
        return "SELECT * FROM nsdpmetadata WHERE NSDPIndicatorID = %s"

    def fix_year(self, year):
        return int(year)

    def fix_value(self, value):
        return int(value.replace('%', ''))

    def fix_indicator_id(self, indicator_id):
        return indicator_id.replace(' ', '').replace('.', '-')
