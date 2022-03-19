package cl.uchile.dcc.caching.utils;

public class TestLoops {
  public static void main(String[] args) {
    for (int i = 1; i <= 5; i++) {
      System.out.println(i);
      while (true) {
        break;
      }
    }
  }
}
