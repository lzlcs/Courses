from operator import add, mul

square = lambda x: x * x

identity = lambda x: x

triple = lambda x: 3 * x

increment = lambda x: x + 1


def ordered_digits(x):
    """Return True if the (base 10) digits of X>0 are in non-decreasing
    order, and False otherwise.

    >>> ordered_digits(5)
    True
    >>> ordered_digits(11)
    True
    >>> ordered_digits(127)
    True
    >>> ordered_digits(1357)
    True
    >>> ordered_digits(21)
    False
    >>> result = ordered_digits(1375) # Return, don't print
    >>> result
    False

    """
    "*** YOUR CODE HERE ***"
    pre = 10 
    while (x != 0):
        tmp = x % 10
        if (pre < tmp):
            return False
        pre, x = tmp, x // 10
    return True


def get_k_run_starter(n, k):
    """Returns the 0th digit of the kth increasing run within n.
    >>> get_k_run_starter(123444345, 0) # example from description
    3
    >>> get_k_run_starter(123444345, 1)
    4
    >>> get_k_run_starter(123444345, 2)
    4
    >>> get_k_run_starter(123444345, 3)
    1
    >>> get_k_run_starter(123412341234, 1)
    1
    >>> get_k_run_starter(1234234534564567, 0)
    4
    >>> get_k_run_starter(1234234534564567, 1)
    3
    >>> get_k_run_starter(1234234534564567, 2)
    2
    """
    i, final = 0, 10
    while (n != 0 and i <= k):
        final, i = 10, i + 1
        while (n % 10 < final and n != 0):
            final, n = n % 10, n // 10
    return final


def make_repeater(func, n):
    """Return the function that computes the nth application of func.

    >>> add_three = make_repeater(increment, 3)
    >>> add_three(5)
    8
    >>> make_repeater(triple, 5)(1) # 3 * 3 * 3 * 3 * 3 * 1
    243
    >>> make_repeater(square, 2)(5) # square(square(5))
    625
    >>> make_repeater(square, 4)(5) # square(square(square(square(5))))
    152587890625
    >>> make_repeater(square, 0)(5) # Yes, it makes sense to apply the function zero times!
    5
    """
    def f(x):
        tmp = n
        while (tmp != 0):
            x, tmp = func(x), tmp - 1
        return x
    return f

def composer(func1, func2):
    """Return a function f, such that f(x) = func1(func2(x))."""
    def f(x):
        return func1(func2(x))
    return f


def apply_twice(func):
    """ Return a function that applies func twice.

    func -- a function that takes one argument

    >>> apply_twice(square)(2)
    16
    """
    "*** YOUR CODE HERE ***"
    def f(x):
        return make_repeater(func, x)(x)
    return f


def div_by_primes_under(n):
    """
    >>> div_by_primes_under(10)(11)
    False
    >>> div_by_primes_under(10)(121)
    False
    >>> div_by_primes_under(10)(12)
    True
    >>> div_by_primes_under(5)(1)
    False
    """

    # Why lambda?
    # Can't understand
    def f(x):
        for i in range(2, n + 1):
            if (x % i == 0):
                return True
        return False
    return f


def div_by_primes_under_no_lambda(n):
    """
    >>> div_by_primes_under_no_lambda(10)(11)
    False
    >>> div_by_primes_under_no_lambda(10)(121)
    False
    >>> div_by_primes_under_no_lambda(10)(12)
    True
    >>> div_by_primes_under_no_lambda(5)(1)
    False
    """
    def f(x):
        for i in range(2, n + 1):
            if (x % i == 0):
                return True
        return False
    return f


def zero(f):
    return lambda x: x


def successor(n):
    # def noname1(f):
        # def noname2(x):
            # return f(n(f)(x))
        # return noname2
    # return noname1
    return lambda f: lambda x: f(n(f)(x))

def one(f):
    """Church numeral 1: same as successor(zero)"""
    "*** YOUR CODE HERE ***"
    
    def func(x):
        return f(x)
    return func

def two(f):
    """Church numeral 2: same as successor(successor(zero))"""
    "*** YOUR CODE HERE ***"

    def func(x):
        return f(f(x))
    return func


three = successor(two)


def church_to_int(n):
    """Convert the Church numeral n to a Python integer.

    >>> church_to_int(zero)
    0
    >>> church_to_int(one)
    1
    >>> church_to_int(two)
    2
    >>> church_to_int(three)
    3
    """
    "*** YOUR CODE HERE ***"
    def add1(x):
        return x + 1
    return n(add1)(0)


def int_to_church(x):
    tmp = zero
    for i in range(0, x):
        tmp = successor(tmp)
    return tmp 

def add_church(m, n):
    """Return the Church numeral for m + n, for Church numerals m and n.

    >>> church_to_int(add_church(two, three))
    5
    """
    "*** YOUR CODE HERE ***"
    a = church_to_int(m)
    b = church_to_int(n)
    return int_to_church(a + b)

# print(add_church(one, two))
def mul_church(m, n):
    """Return the Church numeral for m * n, for Church numerals m and n.

    >>> four = successor(three)
    >>> church_to_int(mul_church(two, three))
    6
    >>> church_to_int(mul_church(three, four))
    12
    """
    "*** YOUR CODE HERE ***"
    a = church_to_int(m)
    b = church_to_int(n)
    return int_to_church(a * b)


def pow_church(m, n):
    """Return the Church numeral m ** n, for Church numerals m and n.

    >>> church_to_int(pow_church(two, three))
    8
    >>> church_to_int(pow_church(three, two))
    9
    """
    "*** YOUR CODE HERE ***"
    a = church_to_int(m)
    b = church_to_int(n)
    return int_to_church(pow(a, b))

