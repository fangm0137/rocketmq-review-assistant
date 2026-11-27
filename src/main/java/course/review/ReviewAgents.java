package course.review;

import com.sun.source.tree.BinaryTree;
import com.sun.source.tree.CatchTree;
import com.sun.source.tree.CompilationUnitTree;
import com.sun.source.tree.ExpressionTree;
import com.sun.source.tree.LiteralTree;
import com.sun.source.tree.MethodInvocationTree;
import com.sun.source.tree.MethodTree;
import com.sun.source.tree.ReturnTree;
import com.sun.source.tree.Tree;
import com.sun.source.tree.VariableTree;
import com.sun.source.util.JavacTask;
import com.sun.source.util.SourcePositions;
import com.sun.source.util.TreeScanner;
import com.sun.source.util.Trees;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

final class ReviewAgents {
    record Finding(String rule, String severity, long line, String observation, String recommendation) {}
    record Review(String agent, List<Finding> findings) {}
    record Task(String session, String agent, String sourceName, String source, List<Review> reviews) {}
    record Result(String session, String agent, Review review, String report) {}

    interface Agent {
        String role();
        Result execute(Task task) throws IOException;
    }

    abstract static class SourceAgent implements Agent {
        @Override
        public final Result execute(Task task) throws IOException {
            var messages = new DiagnosticCollector<JavaFileObject>();
            JavaFileObject source = new SimpleJavaFileObject(
                    URI.create("string:///ReviewInput.java"), JavaFileObject.Kind.SOURCE) {
                @Override
                public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                    return task.source();
                }
            };
            var compiler = ToolProvider.getSystemJavaCompiler();
            try (var files = compiler.getStandardFileManager(messages, null, null)) {
                JavacTask javac = (JavacTask) compiler.getTask(null, files, messages,
                        List.of("-proc:none"), null, List.of(source));
                CompilationUnitTree unit = javac.parse().iterator().next();
                List<Finding> findings = new ArrayList<>();
                for (var message : messages.getDiagnostics()) {
                    if (message.getKind() == Diagnostic.Kind.ERROR) {
                        findings.add(new Finding("JAVA-PARSE", "INFO", message.getLineNumber(),
                                "Java parser reported a syntax error.",
                                "Provide a complete Java source or wrap the snippet in a class."));
                    }
                }
                if (findings.isEmpty()) {
                    inspect(unit, Trees.instance(javac).getSourcePositions(), findings);
                }
                findings.sort(Comparator.comparingLong(Finding::line).thenComparing(Finding::rule));
                return new Result(task.session(), role(), new Review(role(), List.copyOf(findings)), null);
            }
        }

        abstract void inspect(CompilationUnitTree unit, SourcePositions positions, List<Finding> findings);

        final void add(CompilationUnitTree unit, SourcePositions positions, List<Finding> findings,
                       Tree node, String rule, String severity, String observation, String recommendation) {
            long offset = positions.getStartPosition(unit, node);
            long line = offset < 0 ? 0 : unit.getLineMap().getLineNumber(offset);
            findings.add(new Finding(rule, severity, line, observation, recommendation));
        }
    }

    static final class StyleAgent extends SourceAgent {
        public String role() { return "STYLE"; }

        @Override
        void inspect(CompilationUnitTree unit, SourcePositions positions, List<Finding> findings) {
            new TreeScanner<Void, Void>() {
                private void name(Tree tree, String value) {
                    if (!value.matches("[a-z][A-Za-z0-9]*")) {
                        add(unit, positions, findings, tree, "STYLE-01", "LOW",
                                "Name does not follow the lowerCamelCase convention.",
                                "Use a descriptive lowerCamelCase name.");
                    }
                }

                @Override
                public Void visitMethod(MethodTree node, Void ignored) {
                    if (node.getReturnType() != null) {
                        name(node, node.getName().toString());
                    }
                    for (VariableTree parameter : node.getParameters()) {
                        name(parameter, parameter.getName().toString());
                    }
                    return super.visitMethod(node, ignored);
                }
            }.scan(unit, null);
        }
    }

    static final class LogicAgent extends SourceAgent {
        public String role() { return "LOGIC"; }

        @Override
        void inspect(CompilationUnitTree unit, SourcePositions positions, List<Finding> findings) {
            new TreeScanner<Void, Void>() {
                @Override
                public Void visitBinary(BinaryTree node, Void ignored) {
                    if (node.getKind() == Tree.Kind.DIVIDE && !(node.getRightOperand() instanceof LiteralTree)) {
                        add(unit, positions, findings, node, "LOGIC-01", "MEDIUM",
                                "Division uses a variable denominator.",
                                "Verify the zero-denominator path and add a boundary test.");
                    }
                    if (node.getKind() == Tree.Kind.LESS_THAN_EQUAL
                            && node.getRightOperand().toString().matches(".*(?:\\.size\\(\\)|\\.length)")) {
                        add(unit, positions, findings, node, "LOGIC-02", "MEDIUM",
                                "Inclusive upper bound may admit an index equal to the collection size.",
                                "Check the index range and test empty and one-element collections.");
                    }
                    return super.visitBinary(node, ignored);
                }

                @Override
                public Void visitCatch(CatchTree node, Void ignored) {
                    if (node.getBlock().getStatements().isEmpty()) {
                        add(unit, positions, findings, node, "LOGIC-03", "MEDIUM",
                                "Exception handler is empty.",
                                "Handle or propagate the exception and test the failure path.");
                    }
                    return super.visitCatch(node, ignored);
                }
            }.scan(unit, null);
        }
    }

    static final class SecurityAgent extends SourceAgent {
        private static final Set<String> SQL_CALLS = Set.of(
                "prepareStatement", "executeQuery", "executeUpdate", "execute");
        public String role() { return "SECURITY"; }

        @Override
        void inspect(CompilationUnitTree unit, SourcePositions positions, List<Finding> findings) {
            new TreeScanner<Void, Void>() {
                @Override
                public Void visitMethodInvocation(MethodInvocationTree node, Void ignored) {
                    String select = node.getMethodSelect().toString();
                    String name = select.substring(select.lastIndexOf('.') + 1);
                    if (SQL_CALLS.contains(name) && !node.getArguments().isEmpty()
                            && concatenatesValue(node.getArguments().get(0))) {
                        add(unit, positions, findings, node, "SEC-01", "HIGH",
                                "SQL call receives a concatenated expression.",
                                "Use a parameterized statement and bind untrusted values separately.");
                    }
                    return super.visitMethodInvocation(node, ignored);
                }

                @Override
                public Void visitReturn(ReturnTree node, Void ignored) {
                    ExpressionTree value = node.getExpression();
                    if (concatenatesValue(value) && value.toString().matches("(?s).*<[a-zA-Z].*")) {
                        add(unit, positions, findings, node, "SEC-02", "MEDIUM",
                                "HTML markup is assembled with a variable value.",
                                "Check whether this reaches an HTML sink. Use contextual escaping or an auto-escaping template.");
                    }
                    return super.visitReturn(node, ignored);
                }
            }.scan(unit, null);
        }

        private static boolean concatenatesValue(ExpressionTree expression) {
            return expression instanceof BinaryTree binary && binary.getKind() == Tree.Kind.PLUS
                    && (hasDynamicValue(binary.getLeftOperand()) || hasDynamicValue(binary.getRightOperand()));
        }

        private static boolean hasDynamicValue(ExpressionTree expression) {
            if (expression instanceof BinaryTree binary && binary.getKind() == Tree.Kind.PLUS) {
                return hasDynamicValue(binary.getLeftOperand()) || hasDynamicValue(binary.getRightOperand());
            }
            return !(expression instanceof LiteralTree);
        }
    }

    static final class SummaryAgent implements Agent {
        private static final List<String> ORDER = List.of("STYLE", "LOGIC", "SECURITY");
        public String role() { return "SUMMARY"; }

        @Override
        public Result execute(Task task) {
            Map<String, Review> reviews = new java.util.HashMap<>();
            for (Review review : task.reviews()) {
                if (reviews.putIfAbsent(review.agent(), review) != null) {
                    throw new IllegalArgumentException("Duplicate review " + review.agent());
                }
            }
            if (!reviews.keySet().equals(Set.copyOf(ORDER))) {
                throw new IllegalArgumentException("Expected Style, Logic and Security reviews");
            }
            StringBuilder report = new StringBuilder("# Code Review\n\n")
                    .append("- Session: ").append(task.session()).append('\n')
                    .append("- Input: ").append(task.sourceName()).append('\n')
                    .append("- Reviewers: 3/3\n")
                    .append("- Findings: ").append(reviews.values().stream()
                            .mapToInt(r -> r.findings().size()).sum()).append('\n')
                    .append("- Scope: static Java checks. Findings require human confirmation.\n");
            for (String role : ORDER) {
                report.append("\n## ").append(role).append('\n');
                List<Finding> findings = reviews.get(role).findings();
                if (findings.isEmpty()) {
                    report.append("- No findings from this agent.\n");
                }
                for (Finding finding : findings) {
                    report.append("- ").append(finding.severity()).append(' ').append(finding.rule())
                            .append(" (line ").append(finding.line()).append("): ")
                            .append(finding.observation()).append(' ')
                            .append(finding.recommendation()).append('\n');
                }
            }
            return new Result(task.session(), role(), null, report.toString());
        }
    }
}
