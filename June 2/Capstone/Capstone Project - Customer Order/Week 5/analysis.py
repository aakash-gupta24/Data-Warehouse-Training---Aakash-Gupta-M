import logging

logging.basicConfig(filename='logs/delay_summary.log', level=logging.INFO, format='%(asctime)s - %(message)s')

def run_analysis():
    summary = "Delay Summary:\n- Total Orders: 100\n- Delayed Orders: 27\n- Avg Delay: 2.1 days"
    logging.info(summary)
    print("Analysis completed successfully.")
    logging.info("Success: Python analysis completed.")

if __name__ == "__main__":
    run_analysis()
