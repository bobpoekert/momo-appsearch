package org.jsoup.select;

import java.util.Collection;

public class CombiningEvaluatorWrapper {

    public static CombiningEvaluator.And makeAnd(Collection<Evaluator> inp) {
        return new CombiningEvaluator.And(inp);
    }

    public static CombiningEvaluator.Or makeOr(Iterable<Evaluator> inp) {
        CombiningEvaluator.Or res = new CombiningEvaluator.Or();
        for (Evaluator item : inp) {
            res.add(item);
        }
        return res;
    }

}
