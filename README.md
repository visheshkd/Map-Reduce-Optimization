# Map-Reduce-Optimization
The purpose of this project is to improve the performance of k-means clustering by using in-mapper combining..

pseudo code :

class Point {
    public double x;
    public double y;
}

class Avg {
    public double sumX;
    public double sumY;
    public long count;
}

Vector[Point] centroids;
Hashtable[Point,Avg] table;

mapper setup:
  read centroids from the distributed cache
  initialize table

mapper cleanup:
  for each key c in table
      emit(c,table[c])

map ( key, line ):
  Point p = new Point()
  read 2 double numbers from the line (x and y) and store them in p
  find the closest centroid c to p
  if table[c] is empty
     then table[c] = new Avg(x,y,1)
     else table[c] = new Avg(table[c].sumX+x,table[c].sumY+y,table[c].count+1)

reduce ( c, avgs ):
  count = 0
  sx = sy = 0.0
  for a in avgs
      sx += a.sumX
      sy += a.sumY
      count += a.count
  c.x = sx/count
  c.y = sy/count
  emit(c,null)
