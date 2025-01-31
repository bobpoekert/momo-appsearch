package co.momomo;

import org.jsoup.nodes.Element;
import org.jsoup.select.*;

/**
 * Base structural evaluator.
 */
abstract class StructuralEvaluator extends Evaluator {
    Evaluator evaluator;

    public static class Root extends Evaluator {
        public boolean matches(Element root, Element element) {
            return root == element;
        }
    }

    public static class Has extends StructuralEvaluator {
        public Has(Evaluator evaluator) {
            this.evaluator = evaluator;
        }

        public boolean matches(Element root, Element element) {
            for (Element e : element.getAllElements()) {
                if (e != element && evaluator.matches(root, e))
                    return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return String.format(":has(%s)", evaluator);
        }
    }

    public static class Not extends StructuralEvaluator {
        public Not(Evaluator evaluator) {
            this.evaluator = evaluator;
        }

        public boolean matches(Element root, Element node) {
            return !evaluator.matches(root, node);
        }

        @Override
        public String toString() {
            return String.format(":not%s", evaluator);
        }
    }

    public static class Parent extends StructuralEvaluator {
        public Parent(Evaluator evaluator) {
            this.evaluator = evaluator;
        }

        public boolean matches(Element root, Element element) {
            if (root == element)
                return false;

            Element parent = element.parent();
            while (true) {
                if (evaluator.matches(root, parent))
                    return true;
                if (parent == root)
                    break;
                parent = parent.parent();
            }
            return false;
        }

        @Override
        public String toString() {
            return String.format(":parent%s", evaluator);
        }
    }

    public static class ImmediateParent extends StructuralEvaluator {
        public ImmediateParent(Evaluator evaluator) {
            this.evaluator = evaluator;
        }

        public boolean matches(Element root, Element element) {
            if (root == element)
                return false;

            Element parent = element.parent();
            return parent != null && evaluator.matches(root, parent);
        }

        @Override
        public String toString() {
            return String.format(":ImmediateParent%s", evaluator);
        }
    }

    public static class PreviousSibling extends StructuralEvaluator {
        public PreviousSibling(Evaluator evaluator) {
            this.evaluator = evaluator;
        }

        public boolean matches(Element root, Element element) {
            if (root == element)
                return false;

            Element prev = element.previousElementSibling();

            while (prev != null) {
                if (evaluator.matches(root, prev))
                    return true;

                prev = prev.previousElementSibling();
            }
            return false;
        }

        @Override
        public String toString() {
            return String.format(":prev*%s", evaluator);
        }
    }

    public static class ImmediatePreviousSibling extends StructuralEvaluator {
        public ImmediatePreviousSibling(Evaluator evaluator) {
            this.evaluator = evaluator;
        }

        public boolean matches(Element root, Element element) {
            if (root == element)
                return false;

            Element prev = element.previousElementSibling();
            return prev != null && evaluator.matches(root, prev);
        }

        @Override
        public String toString() {
            return String.format(":prev%s", evaluator);
        }
    }
}
