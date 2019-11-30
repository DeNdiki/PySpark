pyspark --master yarn \
--conf spark.ui.port=12890 \
--num-executors 2 \
--executor-memory 512M \
--packages com.databricks:spark-avro_2.10:2.0.1


from pyspark.sql import functions as F, Row

clients_rdd = sc.textFile("/user/iskibongue/cca2017/clients/clients")
produits_rdd = sc.textFile("/user/iskibongue/cca2017/clients/bkprod")


clients_df = clients_rdd.filter(lambda p: p.split('|')[0].encode('utf-8').strip() != 'AGENCE').\
map(lambda o: (o.split('|')[0], o.split('|')[2], o.split('|')[3], o.split('|')[5], o.split('|')[7], o.split('|')[13], \
int(o.split('|')[14].encode("utf-8")), o.split('|')[15], o.split('|')[16], float((o.split('|')[18]).encode('utf-8').\
replace(',','.')), float((o.split('|')[19]).encode('utf-8').replace(',','.')), o.split('|')[22].encode('utf-8'))).\
map(lambda o: Row(agence = o[0], dte_naiss = o[1], sexe = o[2], statut_matrimonial = o[3], nationalite = o[4], type_client = o[5],\
cpro = o[6], dte_ouverture = o[7], compte = o[8], solde = o[9], solde_arete = o[10], nom_client = o[11])).toDF()


produits_df = produits_rdd.filter(lambda p: p.split('|')[0].encode('utf-8').strip() != 'CPRO').\
map(lambda o : Row(cpro = int(o.split('|')[0].encode("utf-8")), libpro = o.split('|')[1].encode("utf-8"))).toDF()
+----+--------------------+
|cpro|              libpro|
+----+--------------------+
|   1|COMPTE CHï¿½QUE P...|
|   2|COMPTE COURANT SO...|
|   3|COMPTE CHï¿½QUE P...|
|   4|COMPTE CHEQUE AGE...|
+----+--------------------+

#TOP 20 des produits les plus vendu de CCA à sa clientèle
#format du résultat : code_produit, libelle_produit, nombre_de_souscription
trans = clients_df.groupBy('cpro').agg(F.count('nom_client').alias('nbe_souscription')).\
orderBy(F.desc('nbe_souscription'))

trans.join(produits_df, trans.cpro == produits_df.cpro, 'inner').select(trans.cpro, produits_df.libpro, trans.nbe_souscription).\
orderBy(F.desc('nbe_souscription')).show(150)

#TOP 100 des comptes clients avec les soldes les plus elevés
clients_df.groupBy('compte','nom_client').agg(F.sum('solde').alias('solde_total'), F.sum('solde_arete').alias('solde_total_arete')).\
orderBy(F.desc('solde_total')).select('compte','nom_client','solde_total','solde_total_arete').show(100)

#TOP 20 des clients ayant souscris à plus d'un produit
_trans = clients_df.groupBy('compte','nom_client').agg(F.count('cpro').alias('nbe_produit')).\
orderBy(F.desc('nbe_produit')).show(150)

#TOP 100 des agences ayant le plus de clients
clients_df.groupBy('agence').agg(F.count('compte').alias('nombre_de_client')).\
orderBy(F.desc('nombre_de_client')).show(100)

