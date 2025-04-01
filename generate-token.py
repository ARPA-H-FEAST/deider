import sys
import json
import glob
from cryptography.fernet import Fernet




def main():


    token = Fernet.generate_key().decode()
    print (token)
    exit()

    raw = "GWU1111111"
    hash_ = Fernet(token).encrypt(raw.encode()).decode()
    label_count = 123
    label = "V_0000" + str(label_count)
    v_dict = {}
    v_dict[raw] = {
        "label": label,
        "hash":hash_
    }
    print (json.dumps(v_dict, indent=4))
    
    decr = Fernet(token).decrypt(hash_.encode())
    print (decr)



if __name__ == '__main__':
    main()


