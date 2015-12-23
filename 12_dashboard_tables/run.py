import yaml
import os
import sys
with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)


sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_bought_together']))
import all_bought_together
execfile(os.path.join(cfg['root']+cfg['dir_bought_together']+cfg['file_all_bought_together']))


sys.path.insert(0, os.path.join(cfg['root']+cfg['dir_bought_together']))
import monthly_bought_together
execfile(os.path.join(cfg['root']+cfg['dir_bought_together']+cfg['file_monthly_bought_together']))


