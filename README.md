# Edna: Data Disguising and Revealing for User Data in Web Applications

Edna[^*] is a library for web developers to add support for data disguising and
revealing of user data to their applications. For example, Edna can help users
protect inactive accounts, selectively dissociate personal data from public
profiles, and remove service access to their data without permanently losing
their accounts.

Edna was last tested on a machine with 16 CPUs and 60 GB RAM, running Ubuntu 20.04.5 LTS, and uses MySQL with the InnoDB storage engine atop a local SSD. The CloudLab profile (`profile.py`) should provide these settings, but numbers may differ due to variability in the machine type, etc.

## Repository organization:
* `deps/`: third-party libraries that Edna uses for e.g., MySQL parsing
* `edna/`: the Edna library itself
* `edna_cryptdb/`: the Edna library, but meant to work atop an encrypted database (a la CryptDB)
    *  Requires `applications/proxy` to be running in order to work correctly
* `edna_srv/`: Edna's library run as an API server (for the Lobsters E2E deployment)
* `related_systems/qapla`: the variant of Qapla that implements a subset of
    Edna's functionality for comparison.
* `applications/`: implementations of data disguising and revealing for
    benchmarks and case study applications (HotCRP, Lobsters, and WebSubmit).
    * `applications/websubmit-rs` has three different servers: one for Edna,
    Edna-CryptDB, and our Qapla variant respectively.
    * `applications/proxy` runs the proxy for Edna-CryptDB
* `results/`: 
    * `results/[app]`: where benchmarks output raw results 
    * `results/plotters/`: graph-plotting scripts 
    * `results/result_graphs`: output of graph-plotting scripts

The requisite scripts to run benchmarks are all contained in the root directory!

## Running Benchmarks
1. Instantiate the profile in CloudLab: [profile link here](https://www.cloudlab.us/p/Edna/UbuntuRepo).
2. `ssh` into the CloudLab instance
3. Run initialization scripts:
   ```
   cp /local/repository/initialize.sh /data; cd /data/; ./initialize.sh
   ```
4. Install `rust` packages:
   ```
   yes | sudo apt remove rustc cargo
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh #take the default options
   ```
5. Run all benchmarks to produce all results (you might want to run this in a separate terminal session using `tmux`). This will execute per-application benchmark scripts in `applications/[app]`, and then run a graph-plotting script to produce all the graphs from the paper. Graphs will be put in `results/result_graphs`, which you can then `scp` to your local machine from Cloudtop.
   ```
   cd /data/repository; ./run_all.sh
   ``` 

All benchmarks should individually take under 15 minutes to run, with the exception of
Lobsters (which registers and iterates through disguising and revealing all 16k users); this will take 
several hours to complete all trials.

The graphs produced correspond to Figures 6-10 in the paper.

## Case Study Applications

### E2E Lobste.rs
0. Make sure you are using the profile instance.
1. Stop any running mysql instances:
   ```
   sudo service mysql stop
   ```
2. Pull the docker instance:
   ```
   sudo docker pull tslilyai/lobsters-edna:latest
   ```
3. Run the MariaDB docker instance that holds the Lobsters database:
   ```
   sudo docker run --name lobsters_mariadb -v lobsters_data:/var/lib/mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=password -e MYSQL_DATABASE=lobsters -d mariadb
   ```
4. Run the docker instance with the repository mounted at `/edna_srv`; it will print out any logs from Lobsters:
   ```
   sudo docker run -p 3000:3000 -v /data/repository:/edna_srv -ti --user root --link lobsters_mariadb:mariadb --name lobsters_edna tslilyai/lobsters-edna
   ```
5. In another terminal, get a shell:
   ```
   sudo docker exec -ti lobsters_edna /bin/bash
   ```
6. You can observe the Lobste.rs code (and the modification made to add Edna) in the current `/lobsters` current working directory of the shell.
7. In the shell, run the Edna server:
   ```
   cd /edna_srv/edna_srv; ./run_srv.sh
   ```
8. Connect via ssh to the profile experiment instance, with port forwarding:
    ```
   ssh -L 3000:localhost:3000 [instance_url]
    ```
9. Go to `localhost:3000` on your computer to access the Lobsters app
      * Create accounts, post content, and see what happens when you disguise it!
      * Note: the admin account has username `test` and password `test`

_Note: the Docker image runs Edna-fied Lobste.rs from 2021; currently, the image pulls in outdated libraries (causing some visible errors), and work to update this image to the newest version of Lobste.rs is ongoing_

### E2E WebSubmit
0. Make sure you have run `./config_mysql.sh` in the repository root, and are using the profile instance.
1. Run the server:
   ```
   cd applications/websubmit-rs/edna-server; ./run_srv.sh
   ```
3. Connect via ssh to the profile experiment instance, with port forwarding:
   ```
   ssh -L 8000:localhost:8000 [instance_url]
   ```
4. Go to `localhost:8000` on your computer to access the WebSubmit app (no CSS currently used)
   * Login as `tester@admin.edu` to have admin access to add lectures and questions, anonymize users, etc.
   * Create accounts with any other email to submit answers, try deleting your account, etc.

_NOTE: API keys and other disguise IDs are not emailed, but rather printed out as logs on the server. API keys act as a user's "password," and can be used to rederive their private key. Application CSS is not provided yet in this codebase._

## Misc Details
The benchmark scripts rely on the following files (paths can be changed in the scripts if necessary, e.g., if not running on CloudLab):
* `/related_systems/qapla/lib`: contains Qapla library files used in `applications/websubmit-rs/qapla-server/build.rs`. These were built via `make` in `related_systems/qapla` and `related_systems/qapla/examples`; you should not have to rebuild them if using the provided image.
* `/data/lobsters_edna_messages_and_tags.sql`: contains the 
    database with generated Lobsters data for the Lobsters benchmark. Used in
    `applications/lobsters/run_benchmarks.sh`
* Note: `grep -r "data\/repository"` will find all hardcoded paths for running in the given CloudLab profile; change these if running elsewhere.


 [^*]: From Edna of the Incredibles, who designs custom  costumes (disguises) that both enhance superheroes' abilities and keep their civilian identities private!
