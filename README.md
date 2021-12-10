### Installation

To install:
1. Populate `env_vars_template` with usernames and passwords and save in `${HOME}/.env_vars`  
2. Copy public and private `id_ed25519` keys in home directory  
3. Run installation script:
   ```
   ./linux_install.sh --env-vars-file ${HOME}/.env_vars --postgres-version 13
   ```
