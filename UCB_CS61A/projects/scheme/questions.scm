(define (caar x) (car (car x)))
(define (cadr x) (car (cdr x)))
(define (cdar x) (cdr (car x)))
(define (cddr x) (cdr (cdr x)))

;; Problem 15
;; Returns a list of two-element lists
(define (enumerate s)
  (begin
      (define (f t n) 
        (cond ((null? t) '())
              (else (cons (cons n (cons (car t) nil))
                          (f (cdr t) (+ n 1))))))
      (f s 0)
  )
)
;; Problem 16

;; Merge two lists LIST1 and LIST2 according to ORDERED? and return
;; the merged lists.
(define (merge inorder? list1 list2)
  (cond ((null? list1) list2)
        ((null? list2) list1)
        ((inorder? (car list1) (car list2))
            (cons (car list1) (merge inorder? (cdr list1) list2)))
        (else
            (cons (car list2) (merge inorder? list1 (cdr list2)))))
)
;; Optional Problem 2

;; Returns a function that checks if an expression is the special form FORM
(define (check-special form)
  (lambda (expr) (equal? form (car expr))))

(define lambda? (check-special 'lambda))
(define define? (check-special 'define))
(define quoted? (check-special 'quote))
(define let?    (check-special 'let))

;; Converts all let special forms in EXPR into equivalent forms using lambda
(define (let-to-lambda expr)
  (cond ((atom? expr)
         ; BEGIN OPTIONAL PROBLEM 2
         expr
         ; END OPTIONAL PROBLEM 2
         )
        ((quoted? expr)
         ; BEGIN OPTIONAL PROBLEM 2
         expr
         ; END OPTIONAL PROBLEM 2
         )
        ((or (lambda? expr)
             (define? expr))
         (let ((form   (car expr))
               (params (cadr expr))
               (body   (cddr expr)))
           ; BEGIN OPTIONAL PROBLEM 2
           ; (form (let-to-lambda params) (let-to-lambda body))
           (cons form (cons params  
                 (let-to-lambda body)))
           ; END OPTIONAL PROBLEM 2
           ))
        ((let? expr)
         (let ((values (cadr expr))
               (body   (cddr expr)))
           ; BEGIN OPTIONAL PROBLEM 2          
           (let ((name (car (zip values)))
                 (val (cadr (zip values))))
                (cons (cons 'lambda 
                            (cons name 
                                  (cons (let-to-lambda (car body)) 
                                        nil
                                  )
                            )
                      ) 
                      (map let-to-lambda val)
                )
           )
           ; END OPTIONAL PROBLEM 2
          ))
        (else
         ; BEGIN OPTIONAL PROBLEM 2
         
         (cons (car expr) (map let-to-lambda (cdr expr)))
         ; END OPTIONAL PROBLEM 2
         )))

; Some utility functions that you may find useful to implement for let-to-lambda

(define (zip pairs)
    (cond
        ((null? pairs) '(() ()))
        ((null? (cdr pairs)) (map (lambda (x) (cons x nil)) (car pairs)))
        ((null? (cdar pairs)) (cons (map car pairs) nil))
        (else (cons (map car pairs) (zip (map cdr pairs))))
    )
 )


