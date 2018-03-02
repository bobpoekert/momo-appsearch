package co.momomo;

import org.jsoup.select.Evaluator;
import org.jsoup.nodes.Element;
import clojure.lang.IFn;

public class EvaluatorFn extends Evaluator {

    public IFn inner;

    public EvaluatorFn(IFn inner) {
        this.inner = inner;
    }

    @Override
    public boolean matches(Element root, Element element) {
        return (Boolean) this.inner.invoke(root, element);
    }

}
