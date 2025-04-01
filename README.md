## Using the the de-identification tool
This tool follows "Guidance on Satisfying the Safe Harbor Method" to de-identify
PHI fields, satisifying Code of Federal Regulations §164.514(c). At this point,
this tool can be used to deidentify data contained in tabular formats following
the instructions given below.


### Making deidentification config files
Have a look at the example files "/path/to/config/conf/deid.gw.json" and 
"/path/to/config/conf/deid.gw.json" to make a new deidentification 
config file for your new source ("foo"), and name the file 
as "/path/to/config/conf/deid.foo.json"


### Running deidentification script
To use the tool run the command
```
$ python3 deid-files.py -s foo
```

The above script will use input/output paths specified in your 
"/path/to/config/conf/deid.foo.json" file to do the deidentification and will
create "vardb.json" file in the path (also specified in your config file).
The "vardb.json" will contain all the dictionary mapping
of the de-identifyd values (this file should be kept in PRIVATE). 


### Generating token
The file "/path/to/config/conf/token.json" contains the encryption/decryption token
and should be kept in PRIVATE. If you want to generate a new token, run
the following command and update "/path/to/config/conf/token.json":
```
$ python3 generate-token.py 
```



