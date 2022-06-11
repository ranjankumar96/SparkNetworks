from plan import Plan

cron = Plan()

cron.command('top', every='4.hour', output=dict(stdout='E:/Company/SparkNetworks/top_stdout.log',
                                                stderr='/E:/Company/SparkNetworks/top_stderr.log'))
cron.script('main.py', every='1.day', path='/main/SparkNetworks/scripts',
                             environment={'YOURAPP_ENV': 'production'})

if __name__ == "__main__":
    cron.run()