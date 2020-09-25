package common.entity;

public class KeyValue<K,V> {
    private K k;
    private V v;

    public K getK() {
        return k;
    }

    public void setK(K k) {
        this.k = k;
    }

    public V getV() {
        return v;
    }

    public void setV(V v) {
        this.v = v;
    }

    public KeyValue(K k, V v){
        this.k = k;
        this.v = v;
    }

    @Override
    public String toString(){
        return String.format("%s,%s\n",k,v);
    }
}
