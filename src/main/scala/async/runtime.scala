package runtime

import scala.scalanative.runtime.Continuations

type Label[-R] = Continuations.BoundaryLabel[R]

/** Contains a delimited contination, which can be invoked with `resume` */
class Suspension[-T, +R](cont: T => R):
  def resume(arg: T): R = cont(arg)

def suspend[T, R](body: Suspension[T, R] => R)(using Label[R]): T =
  Continuations.suspend[T, R](f => body(Suspension(f)))

inline def boundary[R](inline body: Label[R] ?=> R): R =
  Continuations.boundary(body)
