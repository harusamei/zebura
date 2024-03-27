import configparser

# Create a ConfigParser object
config = configparser.ConfigParser()

# Read the config.ini file
config.read('config.ini')


# Example usage
if __name__ == '__main__':
    value = config.get('General', 'ProjectName')
    print(value)
