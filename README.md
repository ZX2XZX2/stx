### Installation

To install:
1. Populate `env_vars_template` with usernames and passwords and save in `${HOME}/.env_vars`  
2. Copy public and private `id_ed25519` keys in home directory  
3. Run installation script:
   ```
   ./linux_install.sh --env-vars-file ${HOME}/.env_vars --postgres-version 13
   ```
4. Run second installation script:
  ```
  ./linux_setup_2.sh [--usb-drive /path/to/usb_drive] [--jdk-file /path/to/jdk-file] [--db-bkp-file /path/to/db-bkp-file]
  
5. TODO: edit /etc/sudoers file so that DB backup works:
   ```
   # User privilege specification
   root	ALL=(ALL:ALL) ALL
   username	ALL=(ALL) NOPASSWD: /usr/bin/pg_dump
   ```
6. To fix the intraday data, run the following commands in the `pgsql` CLI:
   ```
   create table intraday_5 (like intraday including indexes);
   insert into intraday_5 (select stk, dt - interval '5 min', o, hi, lo, c, v, oi from intraday);
   drop table intraday;
   alter table intraday_5 rename to intraday;
   ```