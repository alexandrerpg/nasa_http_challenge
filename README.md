## Nasa http

## 1 - Qual o objetivo do comando cache em Spark?
R: O uso do comando cache é um fator de relevancia para a melhoria de performance da aplicação Spark, pois permite que resultados de
operações lazy evaluation possam ser armazenados in-memory.

## 2 - O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?
R: Essa divergência de performance é ocasionada devido a diferença entre as arquiteturas do Spark e do MapReduce. Mas fazendo uma analise
mais técnica podemos ressaltar que o Spark se sai melhor em operações do tipo Word Count, K-means, e PageRank, pois o Spark possibilita
o armazenamento em cache, quando os dados conseguem ser suportados em memória. Assim o Spark se torna eficiente e eficaz devido a redução de
de sobrecarga de CPU e I/O e também evitando a sobrecarga em output no HDFS.
O time de engenharia de dados da IBM china publicou um artigo sobre as dezenas de diferenças entre Spark e MapReduce no artigo
[Clash of the Titans: MapReduce vs. Spark for Large Scale Data Analytics](http://www.vldb.org/pvldb/vol8/p2110-shi.pdf).

## 3 - Qual é a função do SparkContext?
R: O SparkContext permite que uma aplicação Spark(Java/Python/Scala) se conecte ao cluster com a ajuda de um gerenciador de recursos
como Yarn por exemplo. O SparkContext também fornece várias funções no Spark, como obter o status atual do Spark Application,
definir a configuração, cancelar um job, cancelar um stage entre diversas outras funções.

## 4 - Explique com suas palavras o que é Resilient Distributed Datasets (RDD):
R: RDD é a principal estrutura de dados do Spark, é imutavél e pode ser particionado. RDD é tolerante a falhas,pois possui rollback automático, recriando a "lineage", além de poder ser executado em varios nós.
Por padrão é persistido em memória mas podemos utilizar persistências híbridas e serializadas assim otimizando performance e recursos de amazenamentos memória/disco.
O RDD também é agnostico a fonte, podendo receber dados de HDFS, Hive, Pig, HBASE entre
outros sources. Não posso deixar de citar a característica de Lazzy Evaluation.

## 5 - GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
R: Geralmente utilizamos o reduceByKey, pois a agregação será passada como parâmetro respeitando o modelo chave:valor. Desta forma
teremos um resultado agrupado por partição. Assim evitamos que gargalos de rede ocorram devido a esta operação e passando apenas o que é valido para os executores que irão chegar a um produto final.
Mas se execultarmos a mesma operação utilizando groupByKey, o Spark irá tranferir todas as chaves o cálculo de resultados parciais não é realizado, dessa forma um volume muito maior de dados é desnecessariamente transferido através dos executores podendo, inclusive, ser maior que a quantidade de memória disponível para o mesmo, o que cria a necessidade de escrita dos dados em disco e resulta em um impacto negativo bastante significante na performance.

## 6 - Explique o que o código Scala abaixo faz. 
```
   1. val textFile = sc . textFile ( "hdfs://..." )
   2. val counts = textFile . flatMap ( line => line . split ( " " ))
   3. . map ( word => ( word , 1 ))
   4. . reduceByKey ( _ + _ )
   5. counts . saveAsTextFile ( "hdfs://..." )
```
R: 1. Este bloco de código recebe um arquivo do tipo **texto** é enviado para a **variável textFile**.

   2. Neste passo as **linhas são transformadas** em um coleção de palavras.
   
   3. Então há um processo de **mapeamento** destas palavras como **chave:valor**. Neste caso a chave é a própria palavra.
   
   4. Este passo representa a operação de **agregação**, pegando cada palavra e as **somando**.
   
   5. Finalizando o bloco de código, vemos que o **RDD com a soma** é salvo em um repositório **HDFS como arquivo texto**.

## Respondendo as perguntas referênte ao PDF do desafio. 

## 1. Número de hosts únicos.
    R:  São 137979 host únicos.
    
## 2. O total de erros 404.
    R: Foram contabilizados **20901** requests que retornaram status 404.
    
## 3. Os 5 URLs que mais causaram erro 404.
    R:      +--------------------+-----+
            |            endpoint|count|
            +--------------------+-----+
            |/pub/winvn/readme...| 2004|
            |/pub/winvn/releas...| 1732|
            |/shuttle/missions...|  682|
            |/shuttle/missions...|  426|
            |/history/apollo/a...|  384|
            +--------------------+-----+
     
## 4. Quantidade de erros 404 por dia. 
       R:     +---+-----+
              |day|count|
              +---+-----+
              |  1|  559|
              |  2|  291|
              |  3|  778|
              |  4|  705|
              |  5|  733|
              |  6| 1013|
              |  7| 1107|
              |  8|  693|
              |  9|  627|
              | 10|  713|
              | 11|  734|
              | 12|  667|
              | 13|  748|
              | 14|  700|
              | 15|  581|
              | 16|  516|
              | 17|  677|
              | 18|  721|
              | 19|  848|
              | 20|  740|
              | 21|  639|
              | 22|  480|
              | 23|  578|
              | 24|  748|
              | 25|  876|
              | 26|  702|
              | 27|  706|
              | 28|  504|
              | 29|  420|
              | 30|  571|
              | 31|  526|
              +---+-----+

## 5. O total de bytes retornados. 
      R: O total de bytes trânsacionados foram 65.524.319.796
