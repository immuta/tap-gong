import traceback
import sys
import json

def log_error(error, config, logger, code=None):
    exc_type, exc_value, exc_traceback = sys.exc_info()
    error_info = {
        'message': str(error),
        'traceback': "".join(traceback.format_tb(exc_traceback))
    }

    if code is not None:
        error_info['code'] = code

    error_file_path = config.get('error_file_path', None)
    if error_file_path is not None:
        try:
            with open(error_file_path, 'w', encoding='utf-8') as fp:
                json.dump(error_info, fp)
        except:
            pass

    error_info_json = json.dumps(error_info)
    error_start_marker = config.get('error_start_marker', '[target_error_start]')
    error_end_marker = config.get('error_end_marker', '[target_error_end]')
    logger.info(f'{error_start_marker}{error_info_json}{error_end_marker}')