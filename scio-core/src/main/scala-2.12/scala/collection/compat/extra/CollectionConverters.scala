/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package scala.collection.compat.extra

import java.{lang => jl, util => ju}

import scala.collection.mutable
import scala.collection.convert.{AsJavaConverters, AsScalaConverters}

object CollectionConverters extends AsScalaConverters with AsJavaConverters {

  def asScala[A](i: ju.Iterator[A]): Iterator[A] = asScalaIterator(i)

  def asScala[A](i: jl.Iterable[A]): Iterable[A] = iterableAsScalaIterable(i)

  def asScala[A](i: ju.Collection[A]): Iterable[A] = collectionAsScalaIterable(i)

  def asScala[A](l: ju.List[A]): mutable.Buffer[A] = asScalaBuffer(l)

  def asScala[A](s: ju.Set[A]): mutable.Set[A] = asScalaSet(s)

  def asScala[A, B](m: ju.Map[A, B]): mutable.Map[A, B] = mapAsScalaMap(m)

  def asJava[A](i: Iterator[A]): ju.Iterator[A] = asJavaIterator(i)

  def asJava[A](i: Iterable[A]): jl.Iterable[A] = asJavaIterable(i)

  def asJava[A](b: mutable.Buffer[A]): ju.List[A] = bufferAsJavaList(b)

  def asJava[A](s: mutable.Seq[A]): ju.List[A] = mutableSeqAsJavaList(s)

  def asJava[A](s: Seq[A]): ju.List[A] = seqAsJavaList(s)

  def asJava[A](s: mutable.Set[A]): ju.Set[A] = mutableSetAsJavaSet(s)

  def asJava[A](s: Set[A]): ju.Set[A] = setAsJavaSet(s)

  def asJava[A, B](m: mutable.Map[A, B]): ju.Map[A, B] = mapAsJavaMap(m)

  def asJava[A, B](m: Map[A, B]): ju.Map[A, B] = mapAsJavaMap(m)

}
