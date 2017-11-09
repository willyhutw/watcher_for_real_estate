set -xe

sudo apt-get update
sudo apt-get install -y apt-transport-https ca-certificates curl software-properties-common python-pip
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo apt-get update
sudo apt-get install -y docker-ce
sudo systemctl enable docker
sudo usermod -aG docker vagrant
curl -L https://github.com/docker/compose/releases/download/1.13.0/docker-compose-`uname -s`-`uname -m` > /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo sysctl -w vm.max_map_count=262144

cd vagrant/docker
docker-compose up -d
docker-compose ps

cd /home/vagrant/vagrant
sudo apt-get install -y python-pip
sudo pip install -r requirements.txt
sleep 30s
python import_data.py

curl localhost:9200/_cat/indices?v
