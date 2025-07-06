import logging

logging.basicConfig(filename='output.log', level=logging.INFO, format='%(asctime)s - %(message)s')

def run():
    logging.info('Starting pipeline task...')
    print('Supply Chain script ran successfully')
    logging.info('Script execution completed')

if __name__ == '__main__':
    run()
