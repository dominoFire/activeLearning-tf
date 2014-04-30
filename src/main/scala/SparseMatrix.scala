/*
 * El problema
 * Dado un conjunto de elementos, generar una matriz tf-idf
 *
 * Entrada:
 * [ (una, dos, tres),
 *   (tres, cinco, dos),
 *   (ya, llevo, cinco, veces) ]
 *
 * Salida:
 * (una, dos, tres, cinco, ya, llevo, veces)
 *    1,  1,    1,     0,  0,     0,      0
 *    0,  2,    1,     1,  0,     0,      0
 *    0,  0,    0,     1,  1,     1,      1
 *
 * La salida se guarda en texto primero, luego en binario
 *
 */

/*

map (k1,v1) ==> list(k2,v2)
reduce (k2,list(v2)) ==> list(v2)

 Este tipo es lo que tiene que regresar la operacion map
 una matriz con un solo documento

 Luego, con el reduce, juntamos dos matrices y generamos una nueva

 El elemento neutro es una matriz vacia
 */
class DocumentTermMatrix
{
  /*
   En la matriz vacia, le tienes que hacer creer que cuando obtenga
   un elemento no definido regrese 0 (o 1 o lo que tú quieras)
   y así engañamos al naive bayes

   (d_id, seq( (int, int) )
   */

  def create(docString:String): DocumentTermMatrix = {
    return null
  }
}

class SparseMatrix
{

}
