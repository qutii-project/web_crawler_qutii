import configparser
import sys

# config = configparser.ConfigParser()
# config.read('/Users/vaibhav/Developer/TIQUU/crawler/postgres/database.ini')
# print(config.sections())


from configparser import ConfigParser

# filepath=sys.path[0]+'/'
filepath=sys.path[0]+'/tiquu_pg/'

print('filepath',filepath)
def load_config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    print(filepath+filename)
    parser.read(filepath+filename)

    # get section, default to postgresql
    print(parser.sections())

    config = {}

# host='library.clfojyqicnb4.eu-west-2.rds.amazonaws.com'
# database='postgres'
# user='dev_user'
# password='password'
# port='5432'

    # try:
    #     # read values from a section
    #     host = config.get(section, 'host')
    #     database = config.get(section, 'database')
    #     user = config.get(section, 'user')
    #     password = config.get(section, 'password')
    #     port = config.get(section, 'port')
    # except Exception as e:
    #     return str(e)
    
    print('postgresql' in parser)
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return config

if __name__ == '__main__':
    config = load_config()
    print(config)