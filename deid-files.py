import sys
import json
import glob
from optparse import OptionParser
import util
from cryptography.fernet import Fernet
import subprocess





def main():


    usage = "\n%prog  [options]"
    parser = OptionParser(usage,version=" ")
    parser.add_option("-s","--source",action="store",dest="source",help="source [nbcc/gw/]")
    (options,args) = parser.parse_args()
    for file in ([options.source]):
        if not (file):
            parser.print_help()
            sys.exit(0)

    ds_source = options.source


    config_file = "/data/arpah/generated/conf/deid.%s.json" % (ds_source)
    config_obj = json.loads(open(config_file, "r").read())
    token_obj = json.loads(open("/data/arpah/generated/conf/token.json", "r").read())
    config_obj["token"] = token_obj["token"]

    # token in config was generated using:
    #token = Fernet.generate_key().decode()
    
    config_obj["io"]["vardbfile"] = config_obj["io"]["vardb_dir"] + "/vardb.%s.json" % (ds_source) 
    var_dict = util.load_vardb(config_obj["io"]["vardbfile"]) 

    log_file = "logs/deid-%s.log" % (ds_source)
    msg = "started logging"
    util.write_log(log_file, msg, "w")

    for ftype in config_obj["io"]["input"]:
        d = config_obj["io"]["input"][ftype]
        file_list = sorted(glob.glob(d + "*"))
        for in_file in file_list:
            file_name = in_file.split("/")[-1]
            #if file_name not in ["xx_nbcc_user_profiles.tsv"]:
            #    continue
            out_file = config_obj["io"]["output"][ftype] + file_name
            if ftype in ["tsv"]:
                if in_file not in config_obj["file_types"][ftype]:
                    continue
                deid_obj = config_obj["file_types"][ftype][in_file]
                util.deid_tsv_file(in_file, out_file, deid_obj, var_dict, config_obj["token"])
                msg = "created file: %s" % (out_file)
                util.write_log(log_file, msg, "a")
            elif ftype in ["txt", "vcf"]: 
                if ftype not in config_obj["file_types"]:
                    continue
                #deid_obj = config_obj["fields"][ftype]
                #util.deid_vcf_file(in_file, out_file, deid_obj, var_dict, config_obj["token"])
                #msg = "created file: %s" % (out_file)
                #util.write_log(log_file, msg, "a")
            elif ftype in ["mri_reports"]:
                if ftype not in config_obj["file_types"]:
                    continue
                deid_obj = config_obj["file_types"][ftype]
                out_file = out_file.replace(".txt", ".json")
                util.deid_gw_mri_report_file(in_file, out_file, deid_obj, var_dict, config_obj["token"])
                msg = "created file: %s" % (out_file)
                util.write_log(log_file, msg, "a")
            elif ftype in ["ptl_reports"]:
                if ftype not in config_obj["file_types"]:
                    continue
                deid_obj = config_obj["file_types"][ftype]
                out_file = out_file.replace(".xlsx", ".csv")
                util.deid_gw_ptl_report_file(in_file, out_file, deid_obj, var_dict, config_obj["token"])
                msg = "created file: %s" % (out_file)
                util.write_log(log_file, msg, "a")

    util.update_vardb(var_dict, config_obj["io"]["vardbfile"])


if __name__ == '__main__':
    main()


