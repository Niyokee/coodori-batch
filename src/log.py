import logging.config

logging.config.dictConfig({
    'version': 1,

    # フォーマットの設定
    'formatters': {
        'customFormat': {
            'format': 'format=%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
        },
    },
    # ハンドラの設定
    'handlers': {
        'customStreamHandler': {
            'class': 'logging.StreamHandler',
            'formatter': 'customFormat',
            'level': logging.DEBUG
        }
    },

    # ロガーの対象一覧
    'root': {
        'handlers': ['customStreamHandler'],
        'level': logging.WARNING,
    },
    'loggers': {
        'outputLogging': {
            'handlers': ['customStreamHandler'],
            'level': logging.DEBUG,
            'propagate': 0
        }
    }
})


def logging():
    logger = logging.getLogger('__name__')
    logger.debug('debug message')
    logger.info('info message')
    logger.warning('warning message')
    logger.error('error message')
    logger.critical('critical message')
    logger_lesson2.do_something()