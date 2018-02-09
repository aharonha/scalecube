package io.scalecube.utils;

import java.lang.reflect.Constructor;
import java.util.function.Consumer;

public interface CopyingModifier<E> {

  default E apply(Consumer<E> modifier) {
    try {
      // noinspection unchecked
      E cloneable = (E) this;
      Class<?> cloneableClass = cloneable.getClass();
      Constructor<?> copyingConstructor = cloneableClass.getDeclaredConstructor(cloneableClass);
      copyingConstructor.setAccessible(true);
      // noinspection unchecked
      E newInstance = (E) copyingConstructor.newInstance(cloneable);
      modifier.accept(newInstance);
      return newInstance;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
