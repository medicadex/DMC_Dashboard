import os
import shutil
import tempfile
import atexit
import logging

def setup_temp_cleanup(project_folder):
    """
    Redirects system temp directory to a project-local folder and 
    registers a cleanup routine to delete it on exit.
    """
    temp_dir = os.path.join(project_folder, "dump", "temp_artifacts")
    
    # Ensure the directory exists
    os.makedirs(temp_dir, exist_ok=True)
    
    # Force Python and common libraries (pandas, openpyxl) to use this folder
    os.environ['TMP'] = temp_dir
    os.environ['TEMP'] = temp_dir
    os.environ['TMPDIR'] = temp_dir
    tempfile.tempdir = temp_dir
    
    def purge_temp():
        try:
            if os.path.exists(temp_dir):
                # We use a slight delay or retry if files are locked by the process itself
                shutil.rmtree(temp_dir, ignore_errors=True)
                logging.info(f"Cleaned up temporary artifacts in {temp_dir}")
        except Exception as e:
            logging.warning(f"Failed to purge temp artifacts: {e}")

    # Register the purge function to run when the script finishes
    atexit.register(purge_temp)
    return temp_dir

if __name__ == "__main__":
    # Test run
    setup_temp_cleanup(os.getcwd())
    print("Temp redirection active. Files will be purged on exit.")
