#sudo su
#cat /home/peter.buchmann/ws_go/src/github.com/pbberlin/g1/mongostress/shard_start_scripts/hosts
#vi /etc/hosts


# pb165205
i=1
#for ipa in stress01  stress02  stress03  mgos01  mgos02  mgos03  mgod01  mgod02  mgod03  mgod04  mgod05  mgod06 
for ipa in stress01  stress02  stress03
do
 echo "looping over $((i++)) : $ipa"
 scp /home/peter.buchmann/ws_go/src/github.com/pbberlin/g1/mongostress/shard_start_scripts/*  root@$ipa:/etc/
done

support@profitbricks.com
info@profitbricks.com


