import csv
import sys
import json
import boto3
import datetime
import settings
dynamodb = boto3.resource('dynamodb')

from boto3.dynamodb.conditions import Key, Attr


def get_dynamodb_records(table, primary_key_value):
    primary_key = settings.TABLE_DATA[table]['primary_index']
    table = dynamodb.Table(table)
    records = table.query(KeyConditionExpression=Key(primary_key).eq('30040'))
    return records


def converted_dict(input):
    if isinstance(input, dict):
        return {converted_dict(key): converted_dict(value) for key, value in input.iteritems()}
    elif isinstance(input, list):
        return [converted_dict(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input


def created_at(input):
    output = ''
    try:
        input = int(input) / 100
        output = datetime.datetime.fromtimestamp(input)
        output = str(output)
    except Exception:
        pass
    return output

if __name__ == '__main__':
    try:
    table = str(sys.argv[1])
        print settings.TABLE_DATA[table]['data_keys'][2:]
        if table not in settings.TABLE_DATA.keys():
            print "Table name incorrect"
        else:
            csv_name = "{table}.csv".format(table=table)
            with open(csv_name, 'wb') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(settings.TABLE_DATA[table]['data_keys'])
                for primary_key_value in sys.argv[2:]:
                    try:
                        print "processing started for: ", primary_key_value
                        dynamodb_records = get_dynamodb_records(
                            table, primary_key_value)
                        for item in dynamodb_records.get('Items', []):
                            created_at_string = created_at(
                                item.get('created_at', ''))
                            data = converted_dict(item.get('data', {}))
                            for record in data.get('data', []):
                                data = [primary_key_value, created_at_string] + \
                                    [str(record.get(key, ''))
                                     for key in settings.TABLE_DATA[table]['data_keys'][2:]]
                                writer.writerow(data)
                        print "processing completed for: ", primary_key_value
                    except Exception as e:
                        print "processing failed for: ", primary_key_value, " due to ", str(e)
    except Exception as ex:
        print "This scripted failed due to: ", str(ex)
