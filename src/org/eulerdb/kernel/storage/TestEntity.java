package org.eulerdb.kernel.storage;

public class TestEntity {

    int key;
    int value;

    TestEntity(int key, int value) {

        this.key = key;
        this.value = value;
    }

    public boolean equals(Object o) {

        try {
            TestEntity e = (TestEntity) o;
            return e.key == key && e.value == value;
        } catch (ClassCastException e) {
            return false;
        }
    }

    public int hashCode() {

        return key;
    }

    public String toString() {

        return "[key " + key + " value " + value + ']';
    }
}